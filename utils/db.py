"""Database connection pool and utilities for Kingbase/PostgreSQL."""

from __future__ import annotations

import logging
import uuid
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

logger = logging.getLogger("comparator.db")


def quote_ident(name: str) -> str:
    """Quote a SQL identifier, escaping embedded double quotes.

    Every user-supplied schema/table/column name must go through this before
    being interpolated into SQL. Values always go through bind parameters.
    """
    return '"%s"' % str(name).replace('"', '""')


def qualified(schema: str, table: str) -> str:
    return "%s.%s" % (quote_ident(schema), quote_ident(table))


class TableNotFound(Exception):
    """Raised when a requested table cannot be resolved on a node."""


class _HoldCursor:
    """Iterate a `DECLARE ... WITH HOLD` cursor in batches.

    Exists for Kingbase's sqlserver compatibility mode, where psycopg2's
    named cursors cannot be used: psycopg2 decides whether a named cursor is
    allowed from `conn.autocommit`, and that mode requires autocommit to be
    True. A WITH HOLD cursor is declared explicitly, committed (which
    materialises it), and then fetched in batches -- the same streaming
    behaviour without a psycopg2-managed transaction.

    It exposes just enough of the cursor interface for callers that do
    `for row in cur`: iteration, and close().
    """

    def __init__(self, conn, cursor_name: str, itersize: int = 5000):
        self._conn = conn
        self._name = cursor_name
        self._itersize = max(1, int(itersize or 5000))
        self._exhausted = False

    def __iter__(self):
        quoted = quote_ident(self._name)
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            while not self._exhausted:
                cur.execute("FETCH FORWARD %d FROM %s"
                            % (self._itersize, quoted))
                if not cur.description:
                    # A FETCH that fails is swallowed in sqlserver mode.
                    # Without this check it would look exactly like reaching
                    # the end of the result, silently truncating the scan.
                    code = None
                    try:
                        with self._conn.cursor() as probe:
                            probe.execute("SELECT @@ERROR")
                            row = probe.fetchone()
                            code = row[0] if row else None
                    except Exception:
                        pass
                    raise RuntimeError(
                        "FETCH 失败（sqlserver 模式未返回错误信息，@@ERROR=%s）"
                        % code)
                rows = cur.fetchall()
                if not rows:
                    self._exhausted = True
                    break
                for row in rows:
                    yield row
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def close(self):
        try:
            with self._conn.cursor() as cur:
                cur.execute("CLOSE %s" % quote_ident(self._name))
        except Exception:
            pass


class DatabasePool:
    """Simple connection pool for multiple database nodes."""

    def __init__(self, config: dict):
        self._config = config
        self._conns: dict[str, psycopg2.extensions.connection] = {}
        self._table_cache: dict[tuple, str] = {}
        # Kingbase compatibility mode per node ("oracle"/"mysql"/"sqlserver"/
        # "pg"). Determined once per node on first connect.
        self._mode_cache: dict[str, str] = {}

    def _detect_mode(self, name: str, conn) -> str:
        """Return the node's `database_mode`, caching it.

        The mode decides whether the connection may run in transaction mode
        at all: the sqlserver compatibility mode rejects the bare `BEGIN`
        that psycopg2 emits when `autocommit` is off, so every statement --
        including the very first SELECT -- fails with
        `syntax error at end of input`.

        Detection runs with autocommit on (a statement must be allowed to
        execute before we know the answer) and the caller sets the final
        autocommit value afterwards. A failed probe leaves the mode empty,
        which keeps the previous behaviour (transaction mode).
        """
        if name in self._mode_cache:
            return self._mode_cache[name]

        mode = ""
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SHOW database_mode")
                row = cur.fetchone()
                mode = (row[0] if row else "") or ""
        except Exception as e:
            logger.debug("Could not detect database_mode on %s: %s", name, e)
        mode = str(mode).strip().lower()
        self._mode_cache[name] = mode
        return mode

    def is_sqlserver(self, name: str) -> bool:
        """True when this node runs Kingbase's sqlserver compatibility mode."""
        self.get_conn(name)
        return self._mode_cache.get(name) == "sqlserver"

    def timestamp_type(self, name: str) -> str:
        """SQL type to use for a timestamp column on this node.

        In T-SQL `TIMESTAMP` is a rowversion (a binary concurrency token),
        so `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` is rejected. `DATETIME` is
        the datetime type there and accepts the same default. Every other
        mode uses the standard `TIMESTAMP`.
        """
        return "DATETIME" if self.is_sqlserver(name) else "TIMESTAMP"

    def _check_statement(self, name: str, cur, sql: str = ""):
        """Raise if a statement silently failed on a sqlserver-mode node.

        In sqlserver compatibility mode, Kingbase raises parse errors
        normally but swallows execution-time failures: a query against a
        missing table, a division by zero or a unique violation returns no
        rows, no command tag and no exception. Silently treating those as
        "zero rows" would let the comparison report consistency from a
        query that never ran.

        The only trace left is an empty `statusmessage` -- every successful
        statement (including DDL, DML and cursor commands) sets one. `@@ERROR`
        yields a numeric SQLCODE, but the message text is not exposed.
        """
        if self._mode_cache.get(name) != "sqlserver":
            return
        if cur.statusmessage:
            return

        code = None
        try:
            with cur.connection.cursor() as probe:
                probe.execute("SELECT @@ERROR")
                row = probe.fetchone()
                code = row[0] if row else None
        except Exception:
            pass
        raise RuntimeError(
            "SQL 执行失败，但 Kingbase 的 sqlserver 兼容模式未返回错误信息"
            "（@@ERROR=%s）: %s" % (code, " ".join((sql or "").split())[:200]))

    def get_conn(self, name: str):
        """Get or create a connection to a named database node."""
        if name not in self._conns or self._conns[name].closed:
            dbcfg = self._config["databases"][name]
            conn = psycopg2.connect(
                host=dbcfg["host"],
                port=dbcfg.get("port", 54321),
                dbname=dbcfg["dbname"],
                user=dbcfg["user"],
                password=dbcfg.get("password", ""),
                connect_timeout=dbcfg.get("connect_timeout", 10),
            )
            mode = self._detect_mode(name, conn)
            # sqlserver mode: no implicit BEGIN, so every statement runs and
            # commits on its own. Reads are unaffected; writes already used
            # autocommit before this change.
            conn.autocommit = (mode == "sqlserver")
            if mode == "sqlserver":
                logger.debug("Node %s runs in sqlserver mode; using autocommit", name)
            self._conns[name] = conn
        return self._conns[name]

    def get_database_name(self, name: str) -> str:
        """The dbname this node connects to (used for scope matching)."""
        return self._config["databases"][name].get("dbname", "")

    @contextmanager
    def cursor(self, name: str, autocommit: bool = False):
        """Get a cursor from a named database node. Use as context manager."""
        conn = self.get_conn(name)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            yield cur
            if autocommit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def execute(self, name: str, sql: str, params=None, fetch: bool = True):
        """Execute SQL on a named node and optionally return results."""
        with self.cursor(name, autocommit=not fetch) as cur:
            if params and isinstance(params, list) and len(params) > 0 and isinstance(params[0], (list, tuple)):
                cur.executemany(sql, params)
            else:
                cur.execute(sql, params)
            self._check_statement(name, cur, sql)
            if fetch:
                rows = cur.fetchall()
                # End the read transaction. Without this the connection sits
                # "idle in transaction" between reads, which holds back
                # vacuum on the primary and keeps a snapshot alive on a
                # standby. The statement was a read, so rolling back loses
                # nothing.
                try:
                    self.get_conn(name).rollback()
                except Exception:
                    pass
                return rows
            return None

    def execute_ddl(self, name: str, sql: str):
        """Execute DDL, auto-committed.

        psycopg2 refuses to change `autocommit` while a transaction is open
        ("set_session cannot be used inside a transaction"), so any read
        transaction left by an earlier statement is ended first. The original
        autocommit value is restored rather than hard-coding False -- on a
        sqlserver-mode node it starts as True, and forcing it back to False
        would break every statement that follows.
        """
        conn = self.get_conn(name)
        previous = conn.autocommit
        if not previous:
            try:
                conn.rollback()
            except Exception:
                pass
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                self._check_statement(name, cur, sql)
        finally:
            conn.autocommit = previous

    @contextmanager
    def stream_cursor(self, name: str, sql: str, params=None, itersize: int = 5000):
        """Server-side cursor for streaming a large result set.

        `execute()` always calls fetchall(), which materialises the whole
        result in memory. Comparing large tables needs rows to arrive in
        batches instead.

        Uses a DEDICATED connection rather than the shared cached one. Named
        cursors live inside a transaction, so two threads streaming the same
        node through the shared connection would roll each other's
        transaction out from under them. Table comparisons run in parallel,
        so that collision is the normal case, not an edge case.

        Two implementations, chosen by compatibility mode:

        * normal modes -- psycopg2's named cursor, inside a transaction that
          is rolled back on exit so the read ends promptly (a long read
          transaction holds back vacuum on the primary and can be cancelled
          by `max_standby_streaming_delay` on a standby);

        * sqlserver mode -- psycopg2 refuses named cursors there because it
          keys off `conn.autocommit`, which must be True in that mode. A
          `DECLARE ... WITH HOLD` cursor gives the same batched, server-side
          iteration without needing a psycopg2-managed transaction.
        """
        dbcfg = self._config["databases"][name]
        conn = psycopg2.connect(
            host=dbcfg["host"],
            port=dbcfg.get("port", 54321),
            dbname=dbcfg["dbname"],
            user=dbcfg["user"],
            password=dbcfg.get("password", ""),
            connect_timeout=dbcfg.get("connect_timeout", 10),
        )

        mode = self._mode_cache.get(name)
        if mode is None:
            mode = self._detect_mode(name, conn)

        if mode == "sqlserver":
            # The dedicated connection is new, so its autocommit still has
            # psycopg2's default of False -- the mode cache only tells us
            # what this node needs, it does not apply the setting. Without
            # this the DECLARE below triggers the same implicit BEGIN that
            # sqlserver mode rejects.
            conn.autocommit = True
            cursor_name = "cmp_%s" % uuid.uuid4().hex[:12]
            holder = _HoldCursor(conn, cursor_name, itersize)
            try:
                with conn.cursor() as q:
                    declare_sql = ("DECLARE %s CURSOR WITH HOLD FOR %s"
                                   % (quote_ident(cursor_name), sql))
                    q.execute(declare_sql, params)
                    self._check_statement(name, q, declare_sql)
                    q.execute("COMMIT")
                    self._check_statement(name, q, "COMMIT")
                yield holder
            finally:
                holder.close()
                try:
                    conn.close()
                except Exception:
                    pass
            return

        conn.autocommit = False
        cur = None
        try:
            cur = conn.cursor(name="cmp_%s" % uuid.uuid4().hex[:12],
                              cursor_factory=psycopg2.extras.DictCursor)
            cur.itersize = itersize
            cur.execute(sql, params)
            yield cur
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # -- schema discovery --------------------------------------------------

    def get_schemas(self, name: str) -> list[str]:
        """Non-system schemas.

        Filtered in Python rather than with a LIKE, because psycopg2 treats
        `%` as a bind placeholder and the escaping gets confusing fast. The
        Kingbase-supplied schemas (anon, perf, wmsys, ...) are deliberately
        kept -- deciding which ones are interesting is the job of the
        `scopes` configuration, not of this helper.
        """
        rows = self.execute(name, "SELECT nspname FROM pg_namespace ORDER BY nspname")
        return [r[0] for r in rows
                if not r[0].startswith("pg_") and r[0] != "information_schema"]

    def resolve_table(self, name: str, schema: str, table: str) -> str:
        """Return the table's actual stored name, tolerating case differences.

        Kingbase folds unquoted identifiers (to lowercase on this build) and
        `enable_ci` can be off, so a user typing `-t Accounts` for a table
        stored as `accounts` would previously make the information_schema
        lookups below return an empty list -- silently. That cascaded into
        the checksum engine degrading to row-count-only and the native
        engine comparing every column, with no error raised.

        Resolving the real name up front turns that silent wrong answer into
        either a correct result or an explicit TableNotFound.
        """
        key = (name, schema, table)
        if key in self._table_cache:
            return self._table_cache[key]

        row = self.execute(
            name,
            """SELECT tablename FROM pg_tables
               WHERE schemaname = %s AND tablename = %s""",
            (schema, table),
        )
        if row:
            self._table_cache[key] = row[0][0]
            return row[0][0]

        # Retry with the other case foldings before giving up.
        for candidate in (table.lower(), table.upper()):
            if candidate == table:
                continue
            row = self.execute(
                name,
                """SELECT tablename FROM pg_tables
                   WHERE schemaname = %s AND tablename = %s""",
                (schema, candidate),
            )
            if row:
                logger.debug("Resolved table '%s' -> '%s' on %s", table, row[0][0], name)
                self._table_cache[key] = row[0][0]
                return row[0][0]

        raise TableNotFound(
            "Table %s.%s does not exist on node '%s' "
            "(case-insensitive lookup also failed)" % (schema, table, name)
        )

    def get_tables(self, name: str, schema: str = "public") -> list[str]:
        """Get list of user tables in a schema."""
        rows = self.execute(
            name,
            """SELECT tablename FROM pg_tables
               WHERE schemaname = %s
               ORDER BY tablename""",
            (schema,),
        )
        return [r[0] for r in rows]

    # -- table metadata ----------------------------------------------------

    def get_primary_key(self, name: str, schema: str, table: str) -> list[str]:
        """Get primary key columns for a table.

        Returns an empty list only when the table genuinely has no primary
        key; a missing table raises TableNotFound instead.
        """
        real = self.resolve_table(name, schema, table)
        rows = self.execute(
            name,
            """SELECT kcu.column_name
               FROM information_schema.table_constraints tc
               JOIN information_schema.key_column_usage kcu
                 ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
               WHERE tc.constraint_type = 'PRIMARY KEY'
                 AND tc.table_schema = %s
                 AND tc.table_name = %s
               ORDER BY kcu.ordinal_position""",
            (schema, real),
        )
        return [r[0] for r in rows]

    def get_row_count(self, name: str, schema: str, table: str) -> int:
        """Exact row count for a table."""
        row = self.execute(
            name,
            "SELECT COUNT(*) FROM %s" % qualified(schema, table),
        )
        return row[0][0] if row else 0

    def get_column_info(self, name: str, schema: str, table: str) -> list[dict]:
        """Get column names and declared types for a table, in order."""
        real = self.resolve_table(name, schema, table)
        rows = self.execute(
            name,
            """SELECT column_name, data_type
               FROM information_schema.columns
               WHERE table_schema = %s AND table_name = %s
               ORDER BY ordinal_position""",
            (schema, real),
        )
        return [{"name": r[0], "type": r[1]} for r in rows]

    def get_key_bounds(self, name: str, schema: str, table: str,
                       pk_cols: list[str]):
        """(lowest, highest) primary key tuples, or (None, None) if empty.

        Works for composite keys too, since it sorts by the whole key.
        """
        order = ", ".join(quote_ident(c) for c in pk_cols)
        sel = ", ".join(quote_ident(c) for c in pk_cols)
        target = qualified(schema, table)
        first = self.execute(
            name, "SELECT %s FROM %s ORDER BY %s ASC LIMIT 1" % (sel, target, order))
        last = self.execute(
            name, "SELECT %s FROM %s ORDER BY %s DESC LIMIT 1" % (sel, target, order))
        if not first or not last:
            return None, None
        return tuple(first[0]), tuple(last[0])

    def count_in_range(self, name: str, schema: str, table: str,
                       where_sql: str, params) -> int:
        sql = "SELECT COUNT(*) FROM %s WHERE %s" % (qualified(schema, table), where_sql)
        row = self.execute(name, sql, params)
        return row[0][0] if row else 0

    # -- replication / consistency helpers ---------------------------------

    def is_in_recovery(self, name: str) -> bool:
        """True when this node is a standby."""
        try:
            row = self.execute(name, "SELECT pg_is_in_recovery()")
            return bool(row[0][0]) if row else False
        except Exception:
            return False

    def current_wal_lsn(self, name: str):
        """Primary-side WAL insert location. None on a standby."""
        for fn in ("sys_current_wal_lsn", "pg_current_wal_lsn"):
            try:
                row = self.execute(name, "SELECT %s()" % fn)
                if row and row[0][0] is not None:
                    return row[0][0]
            except Exception:
                continue
        return None

    def last_wal_replay_lsn(self, name: str):
        """Standby-side replay location. None on a primary."""
        for fn in ("sys_last_wal_replay_lsn", "pg_last_wal_replay_lsn"):
            try:
                row = self.execute(name, "SELECT %s()" % fn)
                if row and row[0][0] is not None:
                    return row[0][0]
            except Exception:
                continue
        return None

    def wal_lsn_diff(self, name: str, lsn_a, lsn_b):
        """Byte distance between two LSNs on this node, or None if unsupported."""
        for fn in ("sys_wal_lsn_diff", "pg_wal_lsn_diff"):
            try:
                row = self.execute(
                    name, "SELECT %s(%%s, %%s)" % fn, (lsn_a, lsn_b))
                if row:
                    return row[0][0]
            except Exception:
                continue
        return None

    def table_change_counter(self, name: str, schema: str, tables: list[str]):
        """Sum of n_tup_ins + n_tup_upd + n_tup_del over the given tables.

        Used to decide whether the tables being compared are quiescent.
        Preferred over COUNT(*)/MAX(pk) because those miss UPDATEs entirely,
        and over pg_current_wal_lsn() because autovacuum and checkpoints
        advance WAL without any user-visible data change.

        Returns (total, per_table_dict) or (None, {}) when unsupported.
        """
        if not tables:
            return None, {}
        try:
            rows = self.execute(
                name,
                """SELECT relname, n_tup_ins + n_tup_upd + n_tup_del AS n
                   FROM pg_stat_user_tables
                   WHERE schemaname = %s""",
                (schema,),
            )
        except Exception:
            return None, {}

        per_table = {}
        for r in rows:
            per_table[r[0]] = r[1] or 0
        total = sum(per_table.get(t, 0) for t in tables)
        return total, {t: per_table.get(t, 0) for t in tables}

    # -- lifecycle ---------------------------------------------------------

    def close_all(self):
        """Close all connections."""
        for name, conn in self._conns.items():
            if not conn.closed:
                conn.close()
        self._conns.clear()

    def __del__(self):
        try:
            self.close_all()
        except Exception:
            pass
