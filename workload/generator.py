"""Test data generator for creating reproducible test datasets.

Generates tables and populates them with realistic test data
for consistency verification testing.
"""

import logging
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.db import DatabasePool

logger = logging.getLogger("comparator.generator")

# Predefined table templates
TABLE_TEMPLATES = {
    "accounts": """
        CREATE TABLE IF NOT EXISTS {schema}.accounts (
            id SERIAL PRIMARY KEY,
            account_no VARCHAR(32) NOT NULL UNIQUE,
            account_name VARCHAR(128),
            balance NUMERIC(18,2) DEFAULT 0.00,
            status VARCHAR(16) DEFAULT 'active',
            branch_code VARCHAR(16),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS {schema}.orders (
            id SERIAL PRIMARY KEY,
            order_no VARCHAR(32) NOT NULL UNIQUE,
            account_id INTEGER REFERENCES {schema}.accounts(id),
            product_code VARCHAR(32),
            quantity INTEGER DEFAULT 1,
            amount NUMERIC(18,2),
            order_status VARCHAR(16) DEFAULT 'pending',
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS {schema}.products (
            id SERIAL PRIMARY KEY,
            product_code VARCHAR(32) NOT NULL UNIQUE,
            product_name VARCHAR(256),
            unit_price NUMERIC(18,2),
            stock INTEGER DEFAULT 0,
            category VARCHAR(64),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "transactions": """
        CREATE TABLE IF NOT EXISTS {schema}.transactions (
            id SERIAL PRIMARY KEY,
            txn_no VARCHAR(64) NOT NULL UNIQUE,
            from_account_id INTEGER,
            to_account_id INTEGER,
            amount NUMERIC(18,2),
            txn_type VARCHAR(32),
            txn_status VARCHAR(16) DEFAULT 'completed',
            txn_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            remark TEXT
        )
    """,
}


def _random_str(length: int = 16) -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


def _random_amount(min_val: float = 0.01, max_val: float = 99999.99) -> float:
    return round(random.uniform(min_val, max_val), 2)


class DataGenerator:
    """Generate test data tables and populate with rows."""

    def __init__(self, pool: DatabasePool, node: str, schema: str = "public"):
        self.pool = pool
        self.node = node
        self.schema = schema

    def create_table(self, template_name: str):
        """Create a table from a predefined template."""
        sql = TABLE_TEMPLATES.get(template_name)
        if not sql:
            raise ValueError("Unknown template: %s. Available: %s" % (
                template_name, list(TABLE_TEMPLATES.keys())))
        self.pool.execute_ddl(self.node, sql.format(schema=self.schema))
        logger.info("Created table %s.%s", self.schema, template_name)

    def create_all_tables(self):
        """Create all predefined test tables."""
        for name in TABLE_TEMPLATES:
            self.create_table(name)

    def populate_accounts(self, count: int = 1000, batch_size: int = 500):
        """Insert test account rows."""
        for batch_start in range(0, count, batch_size):
            batch_end = min(batch_start + batch_size, count)
            rows = []
            for i in range(batch_start, batch_end):
                rows.append((
                    "ACC%s" % _random_str(12),
                    "Account_%d" % i,
                    _random_amount(0, 1000000),
                    random.choice(["active", "inactive", "frozen"]),
                    "BR%s" % _random_str(4),
                ))
            self.pool.execute(
                self.node,
                "INSERT INTO %s.accounts "
                "(account_no, account_name, balance, status, branch_code) "
                "VALUES (%%s, %%s, %%s, %%s, %%s)" % self.schema,
                rows,
                fetch=False,
            )
            logger.debug("Accounts: %d/%d", batch_end, count)

    def populate_products(self, count: int = 200, batch_size: int = 200):
        """Insert test product rows."""
        categories = ["Electronics", "Clothing", "Food", "Books", "Sports"]
        for batch_start in range(0, count, batch_size):
            batch_end = min(batch_start + batch_size, count)
            rows = []
            for i in range(batch_start, batch_end):
                rows.append((
                    "PRD%s" % _random_str(10),
                    "Product_%d" % i,
                    _random_amount(1, 9999),
                    random.randint(0, 10000),
                    random.choice(categories),
                ))
            self.pool.execute(
                self.node,
                "INSERT INTO %s.products "
                "(product_code, product_name, unit_price, stock, category) "
                "VALUES (%%s, %%s, %%s, %%s, %%s)" % self.schema,
                rows,
                fetch=False,
            )
            logger.debug("Products: %d/%d", batch_end, count)

    def populate_orders(self, count: int = 5000, batch_size: int = 500):
        """Insert test order rows referencing accounts."""
        # Get account IDs
        acc_rows = self.pool.execute(
            self.node,
            "SELECT id, account_no FROM %s.accounts LIMIT 10000" % self.schema,
        )
        account_ids = [r["id"] for r in acc_rows]

        if not account_ids:
            logger.warning("No accounts found, skipping order generation")
            return

        statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]

        for batch_start in range(0, count, batch_size):
            batch_end = min(batch_start + batch_size, count)
            rows = []
            for i in range(batch_start, batch_end):
                rows.append((
                    "ORD%s" % _random_str(14),
                    random.choice(account_ids),
                    "PRD%s" % _random_str(10),
                    random.randint(1, 100),
                    _random_amount(1, 50000),
                    random.choice(statuses),
                ))
            self.pool.execute(
                self.node,
                "INSERT INTO %s.orders "
                "(order_no, account_id, product_code, quantity, amount, order_status) "
                "VALUES (%%s, %%s, %%s, %%s, %%s, %%s)" % self.schema,
                rows,
                fetch=False,
            )
            logger.debug("Orders: %d/%d", batch_end, count)

    def populate_transactions(self, count: int = 10000, batch_size: int = 500):
        """Insert test transaction rows."""
        types = ["transfer", "deposit", "withdrawal", "payment", "refund"]

        for batch_start in range(0, count, batch_size):
            batch_end = min(batch_start + batch_size, count)
            rows = []
            for i in range(batch_start, batch_end):
                rows.append((
                    "TXN%s" % _random_str(20),
                    random.randint(1, 10000),
                    random.randint(1, 10000),
                    _random_amount(0.01, 500000),
                    random.choice(types),
                ))
            self.pool.execute(
                self.node,
                "INSERT INTO %s.transactions "
                "(txn_no, from_account_id, to_account_id, amount, txn_type) "
                "VALUES (%%s, %%s, %%s, %%s, %%s)" % self.schema,
                rows,
                fetch=False,
            )
            logger.debug("Transactions: %d/%d", batch_end, count)

    def truncate_all(self):
        """Truncate all generated tables before re-populating."""
        for name in TABLE_TEMPLATES:
            try:
                self.pool.execute_ddl(
                    self.node,
                    "TRUNCATE TABLE %s.%s CASCADE" % (self.schema, name),
                )
                logger.info("Truncated table %s.%s", self.schema, name)
            except Exception as e:
                logger.debug("Skip truncate %s: %s", name, e)

    def generate_all(self, accounts: int = 1000, products: int = 200,
                     orders: int = 5000, transactions: int = 10000):
        """Generate all test tables and populate with data."""
        logger.info("Generating test data...")
        self.create_all_tables()

        logger.info("Populating accounts (%d rows)...", accounts)
        self.populate_accounts(accounts)

        logger.info("Populating products (%d rows)...", products)
        self.populate_products(products)

        logger.info("Populating orders (%d rows)...", orders)
        self.populate_orders(orders)

        logger.info("Populating transactions (%d rows)...", transactions)
        self.populate_transactions(transactions)

        logger.info("Test data generation complete!")

    def teardown(self):
        """Drop all generated tables."""
        for name in TABLE_TEMPLATES:
            self.pool.execute_ddl(
                self.node,
                "DROP TABLE IF EXISTS %s.%s CASCADE" % (self.schema, name),
            )
