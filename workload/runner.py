"""Custom workload runner for executing user-defined business scenarios.

Workloads are defined in YAML files with the following structure:

name: "transfer_workload"
description: "模拟金融转账场景"
tables:
  - name: accounts
    ddl: "CREATE TABLE IF NOT EXISTS accounts (...)"
concurrency: 10
duration: 300
transactions:
  - name: "random_transfer"
    weight: 10
    sql: |
      UPDATE accounts SET balance = balance - %(amount)s
      WHERE id = %(from_id)s;
      UPDATE accounts SET balance = balance + %(amount)s
      WHERE id = %(to_id)s;
rollback_on_error: false
"""

from __future__ import annotations

import logging
import time
import random
import threading
import os
import yaml
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.db import DatabasePool

logger = logging.getLogger("comparator.workload")


class WorkloadRunner:
    """Execute custom workload scenarios defined in YAML files."""

    def __init__(self, pool: DatabasePool, node: str, schema: str = "public"):
        self.pool = pool
        self.node = node
        self.schema = schema
        self._stop_event = threading.Event()
        self._stats = {
            "total_ops": 0,
            "success_ops": 0,
            "error_ops": 0,
            "start_time": None,
            "end_time": None,
            "errors": [],
        }
        self._lock = threading.Lock()

    def load_workload(self, yaml_path: str) -> dict:
        """Load a workload definition from a YAML file."""
        with open(yaml_path, "r", encoding="utf-8") as f:
            workload = yaml.safe_load(f)
        self._validate_workload(workload)
        return workload

    def _validate_workload(self, workload: dict):
        """Validate workload definition has required fields."""
        required = ["name", "transactions"]
        for field in required:
            if field not in workload:
                raise ValueError("Workload YAML missing required field: %s" % field)

        for i, txn in enumerate(workload.get("transactions", [])):
            if "sql" not in txn:
                raise ValueError("Transaction %d missing 'sql' field" % i)
            if "name" not in txn:
                txn["name"] = "txn_%d" % i
            if "weight" not in txn:
                txn["weight"] = 1

    def _setup_tables(self, workload: dict):
        """Create tables defined in the workload."""
        for table_def in workload.get("tables", []):
            name = table_def.get("name", "")
            ddl = table_def.get("ddl", "")
            if not ddl:
                continue
            ddl = ddl.replace("{schema}", self.schema)
            try:
                self.pool.execute_ddl(self.node, ddl)
                logger.info("Created table: %s", name)
            except Exception as e:
                logger.warning("Table %s may already exist: %s", name, e)

    def _seed_data(self, workload: dict):
        """Execute seed SQL statements to prepare initial data."""
        seeds = workload.get("seed", [])
        if isinstance(seeds, str):
            seeds = [seeds]

        for sql in seeds:
            sql = sql.format(schema=self.schema)
            try:
                self.pool.execute(self.node, sql, fetch=False)
                logger.debug("Seed executed: %s...", sql[:60])
            except Exception as e:
                logger.error("Seed failed: %s", e)

    def _run_transaction(self, txn_def: dict, conn_name: str):
        """Execute a single transaction (can be multi-statement)."""
        sql_template = txn_def["sql"]

        # Replace placeholders with random values
        sql = self._interpolate_sql(sql_template)

        try:
            with self.pool.cursor(conn_name) as cur:
                # Split by semicolon for multi-statement
                statements = [s.strip() for s in sql.split(";") if s.strip()]
                for stmt in statements:
                    cur.execute(stmt)
            with self._lock:
                self._stats["total_ops"] += 1
                self._stats["success_ops"] += 1
        except Exception as e:
            with self._lock:
                self._stats["total_ops"] += 1
                self._stats["error_ops"] += 1
                if len(self._stats["errors"]) < 100:
                    self._stats["errors"].append({
                        "txn": txn_def.get("name"),
                        "error": str(e),
                        "time": datetime.now().isoformat(),
                    })

    def _interpolate_sql(self, sql: str) -> str:
        """Replace placeholders in SQL with random values.

        Supported placeholders:
          %(random_int:N-M)s  -> random int between N and M
          %(random_str:N)s    -> random string of length N
          %(random_float:N-M)s -> random float
          %(uuid)s            -> UUID
        """
        import re
        import uuid

        # random_int:N-M
        def _repl_int(m):
            n, m_val = m.group(1).split("-")
            return str(random.randint(int(n), int(m_val)))

        sql = re.sub(r'%\(random_int:(\d+-\d+)\)s', _repl_int, sql)

        # random_str:N
        import string
        def _repl_str(m):
            length = int(m.group(1))
            return "'%s'" % ''.join(
                random.choices(string.ascii_letters + string.digits, k=length)
            )

        sql = re.sub(r'%\(random_str:(\d+)\)s', _repl_str, sql)

        # random_float:N-M
        def _repl_float(m):
            n, m_val = m.group(1).split("-")
            return str(round(random.uniform(float(n), float(m_val)), 2))

        sql = re.sub(r'%\(random_float:(\d+\.?\d*-\d+\.?\d*)\)s', _repl_float, sql)

        # uuid
        sql = sql.replace("%(uuid)s", "'%s'" % uuid.uuid4().hex)

        # timestamp
        sql = sql.replace("%(timestamp)s", "'%s'" % datetime.now().isoformat())

        return sql

    def _select_transaction(self, transactions: list[dict]) -> dict:
        """Select a transaction based on weight distribution."""
        total_weight = sum(t.get("weight", 1) for t in transactions)
        r = random.randint(1, total_weight)
        cumulative = 0
        for txn in transactions:
            cumulative += txn.get("weight", 1)
            if r <= cumulative:
                return txn
        return transactions[-1]

    def _worker(self, workload: dict, worker_id: int):
        """Worker thread running transactions."""
        transactions = workload["transactions"]
        duration = workload.get("duration", 300)
        throttle = workload.get("throttle_ms", 0)

        end_time = time.time() + duration

        while not self._stop_event.is_set() and time.time() < end_time:
            txn = self._select_transaction(transactions)
            self._run_transaction(txn, self.node)

            if throttle > 0:
                time.sleep(throttle / 1000.0)

    def run(self, workload: dict) -> dict:
        """Execute a workload definition.

        Args:
            workload: Workload dict (from load_workload or direct)

        Returns:
            Stats dict with execution results.
        """
        name = workload.get("name", "unnamed")
        concurrency = workload.get("concurrency", 10)

        logger.info("Starting workload '%s' with %d workers", name, concurrency)

        # Setup phase
        self._setup_tables(workload)
        self._seed_data(workload)

        self._stop_event.clear()
        self._stats = {
            "total_ops": 0,
            "success_ops": 0,
            "error_ops": 0,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "errors": [],
        }

        # Run workers
        start = time.time()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(self._worker, workload, i)
                for i in range(concurrency)
            ]

            # Wait for completion or timeout
            duration = workload.get("duration", 300)
            try:
                for f in as_completed(futures, timeout=duration + 30):
                    f.result()
            except Exception as e:
                logger.error("Workload interrupted: %s", e)
                self._stop_event.set()

        self._stats["end_time"] = datetime.now().isoformat()
        self._stats["elapsed_seconds"] = round(time.time() - start, 2)
        self._stats["tps"] = round(
            self._stats["total_ops"] / max(self._stats["elapsed_seconds"], 0.001), 2
        )

        logger.info(
            "Workload '%s' finished: %d ops, %d errors, %.2f TPS",
            name, self._stats["total_ops"],
            self._stats["error_ops"], self._stats["tps"],
        )

        return self._stats

    def run_file(self, yaml_path: str) -> dict:
        """Load and run a workload from a YAML file."""
        workload = self.load_workload(yaml_path)
        return self.run(workload)

    def stop(self):
        """Signal the workload to stop."""
        self._stop_event.set()

    def _run_single_statement(self, sql: str):
        """Execute one statement, no stats tracking (for seed data)."""
        try:
            self.pool.execute(self.node, sql, fetch=False)
        except Exception as e:
            logger.debug("Statement failed (non-fatal): %s", e)
