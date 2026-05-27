"""Database connection pool and utilities for Kingbase/PostgreSQL."""

import psycopg2
import psycopg2.extras
from contextlib import contextmanager


class DatabasePool:
    """Simple connection pool for multiple database nodes."""

    def __init__(self, config: dict):
        self._config = config
        self._conns: dict[str, psycopg2.extensions.connection] = {}

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
            conn.autocommit = False
            self._conns[name] = conn
        return self._conns[name]

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
        with self.cursor(name) as cur:
            cur.execute(sql, params)
            if fetch:
                return cur.fetchall()
            return None

    def execute_ddl(self, name: str, sql: str):
        """Execute DDL, auto-committed."""
        conn = self.get_conn(name)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
        finally:
            conn.autocommit = False

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

    def get_primary_key(self, name: str, schema: str, table: str) -> list[str]:
        """Get primary key columns for a table."""
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
            (schema, table),
        )
        return [r[0] for r in rows]

    def get_row_count(self, name: str, schema: str, table: str) -> int:
        """Get approximate or exact row count."""
        row = self.execute(
            name,
            "SELECT COUNT(*) FROM %s.%s" % (schema, table),
        )
        return row[0][0] if row else 0

    def get_column_info(self, name: str, schema: str, table: str) -> list[dict]:
        """Get column names and types for a table."""
        rows = self.execute(
            name,
            """SELECT column_name, data_type
               FROM information_schema.columns
               WHERE table_schema = %s AND table_name = %s
               ORDER BY ordinal_position""",
            (schema, table),
        )
        return [{"name": r[0], "type": r[1]} for r in rows]

    def close_all(self):
        """Close all connections."""
        for name, conn in self._conns.items():
            if not conn.closed:
                conn.close()
        self._conns.clear()

    def __del__(self):
        self.close_all()
