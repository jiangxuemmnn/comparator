"""Three-layer object selection: database / schema / table.

Lets a run skip whole databases, individual schemas, or a handful of tables
inside one schema, e.g. "校验 test 库，但排除其中 3 张表".

Configuration (config.yaml):

    scopes:
      - database: test_db
        include_schemas: [public, biz]
        exclude_schemas: [tmp_*]
        exclude_tables: [tmp_*, "*_bak", _rpo_*]
      - database: audit_db
        exclude_tables: ["*"]          # 整库跳过

Rules:
  - Patterns are shell-style globs (fnmatch): `tmp_*`, `*_bak`, `log_202?`.
  - **exclude always wins over include.**
  - An empty include list means "everything not excluded".
  - A scope whose `database` matches nothing is inert.

Backward compatibility: when `scopes` is absent or empty, a single default
scope is derived from the legacy `comparison.schema` and
`comparison.exclude_tables` keys, so existing config files keep working.

`--exclude-databases` / `--exclude-schemas` / `--exclude-tables` on the
command line are merged on top of the configured rules and can only ever
remove more objects, never add them back -- so `--exclude-tables tmp_x`
still applies even when `-t tmp_x,accounts` names the table explicitly.
"""

from __future__ import annotations

from fnmatch import fnmatch


def match_any(name: str, patterns) -> bool:
    """True if `name` matches any glob in `patterns` (case-insensitive).

    Case-insensitive because Kingbase folds unquoted identifiers and users
    routinely type table names in the wrong case. Being permissive here is
    safe: the real identifier is always re-read from pg_tables /
    information_schema before any SQL is built.
    """
    if not patterns:
        return False
    low = (name or "").lower()
    return any(fnmatch(low, str(p).lower()) for p in patterns if p is not None)


def _as_list(value) -> list:
    """Accept a scalar, a comma-separated string, or a list."""
    if value is None:
        return []
    if isinstance(value, str):
        return [p.strip() for p in value.split(",") if p.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(p) for p in value]
    return [str(value)]


class Scope:
    """One `scopes` entry: which databases it covers and what to keep."""

    def __init__(self, raw: dict):
        self.databases = _as_list(raw.get("database") or raw.get("databases"))
        self.exclude_databases = _as_list(raw.get("exclude_databases"))
        self.include_schemas = _as_list(raw.get("include_schemas"))
        self.exclude_schemas = _as_list(raw.get("exclude_schemas"))
        self.include_tables = _as_list(raw.get("include_tables"))
        self.exclude_tables = _as_list(raw.get("exclude_tables"))

    def covers(self, dbname: str) -> bool:
        """Does this scope apply to `dbname`?

        An empty `database` key means "all databases". A scope that only
        carries `exclude_databases` covers everything and lets the exclusion
        itself decide.
        """
        if self.exclude_databases and not self.databases:
            return True
        if not self.databases:
            return True
        return match_any(dbname, self.databases)

    def database_allowed(self, dbname: str) -> bool:
        return not match_any(dbname, self.exclude_databases)

    def schema_allowed(self, schema: str) -> bool:
        if match_any(schema, self.exclude_schemas):
            return False
        if self.include_schemas:
            return match_any(schema, self.include_schemas)
        return True

    def table_allowed(self, table: str) -> bool:
        if match_any(table, self.exclude_tables):
            return False
        if self.include_tables:
            return match_any(table, self.include_tables)
        return True


class ScopeMatcher:
    """Resolves the configured scopes into per-object allow/deny decisions."""

    def __init__(self, config: dict, default_database: str = None):
        self.default_database = default_database
        self.scopes = []

        for raw in (config.get("scopes") or []):
            if isinstance(raw, dict):
                self.scopes.append(Scope(raw))

        if not self.scopes:
            # Legacy config: comparison.schema + comparison.exclude_tables
            cmp_cfg = config.get("comparison", {}) or {}
            self.scopes.append(Scope({
                "include_schemas": [cmp_cfg.get("schema") or "public"],
                "exclude_tables": _as_list(cmp_cfg.get("exclude_tables")),
            }))

    # -- CLI overrides -----------------------------------------------------

    def add_overrides(self, exclude_databases=None, exclude_schemas=None,
                      exclude_tables=None):
        """Merge command-line exclusions as one extra scope.

        The scope is added unconditionally (even when empty) only if it
        actually excludes something, so a no-op CLI cannot accidentally
        become an allow-list.
        """
        over = Scope({
            "exclude_databases": _as_list(exclude_databases),
            "exclude_schemas": _as_list(exclude_schemas),
            "exclude_tables": _as_list(exclude_tables),
        })
        if over.exclude_databases or over.exclude_schemas or over.exclude_tables:
            self.scopes.append(over)
        return self

    # -- queries -----------------------------------------------------------

    def database_allowed(self, dbname: str) -> bool:
        db = dbname or self.default_database or ""
        for scope in self.scopes:
            if scope.covers(db) and not scope.database_allowed(db):
                return False
        return True

    def schema_allowed(self, dbname: str, schema: str) -> bool:
        db = dbname or self.default_database or ""
        for scope in self.scopes:
            if not scope.covers(db):
                continue
            if not scope.database_allowed(db):
                return False
            if not scope.schema_allowed(schema):
                return False
        return True

    def table_allowed(self, dbname: str, schema: str, table: str) -> bool:
        db = dbname or self.default_database or ""
        for scope in self.scopes:
            if not scope.covers(db):
                continue
            if not scope.database_allowed(db):
                return False
            if not scope.schema_allowed(schema):
                return False
            if not scope.table_allowed(table):
                return False
        return True

    def filter_tables(self, dbname: str, schema: str, tables) -> list:
        """Keep only the tables that survive the rules, preserving order."""
        return [t for t in tables if self.table_allowed(dbname, schema, t)]

    def filter_schemas(self, dbname: str, schemas) -> list:
        return [s for s in schemas if self.schema_allowed(dbname, s)]


def build_matcher(config: dict, args=None, node: str = None) -> ScopeMatcher:
    """Build a matcher from config plus optional CLI args."""
    matcher = ScopeMatcher(config, default_database=node)
    if args is not None:
        matcher.add_overrides(
            exclude_databases=getattr(args, "exclude_databases", None),
            exclude_schemas=getattr(args, "exclude_schemas", None),
            exclude_tables=getattr(args, "exclude_tables", None),
        )
    return matcher
