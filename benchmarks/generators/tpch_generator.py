"""
TPC-H Derived Dataset Generator for Apiary.

Generates the 8-table schema defined by the TPC-H specification (v3.0.1).
This is a "derived" generator — it produces data following TPC-H distributions
and cardinalities but uses numpy for generation rather than the official dbgen tool.
Per TPC fair-use policy, results using this generator should be labeled "TPC-H derived"
rather than "TPC-H" in publications.

Table cardinalities at SF1:
  - lineitem:  ~6,000,000 rows (fact)
  - orders:    1,500,000 rows
  - customer:  150,000 rows
  - part:      200,000 rows
  - partsupp:  800,000 rows
  - supplier:  10,000 rows
  - nation:    25 rows (fixed)
  - region:    5 rows (fixed)
"""

import os
import numpy as np
import pyarrow as pa
from .utils import write_table, print_generation_summary


# --- TPC-H domain constants ---

REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]

NATIONS = [
    ("ALGERIA", "AFRICA"), ("ARGENTINA", "AMERICA"), ("BRAZIL", "AMERICA"),
    ("CANADA", "AMERICA"), ("EGYPT", "MIDDLE EAST"), ("ETHIOPIA", "AFRICA"),
    ("FRANCE", "EUROPE"), ("GERMANY", "EUROPE"), ("INDIA", "ASIA"),
    ("INDONESIA", "ASIA"), ("IRAN", "MIDDLE EAST"), ("IRAQ", "MIDDLE EAST"),
    ("JAPAN", "ASIA"), ("JORDAN", "MIDDLE EAST"), ("KENYA", "AFRICA"),
    ("MOROCCO", "AFRICA"), ("MOZAMBIQUE", "AFRICA"), ("PERU", "AMERICA"),
    ("CHINA", "ASIA"), ("ROMANIA", "EUROPE"), ("SAUDI ARABIA", "MIDDLE EAST"),
    ("VIETNAM", "ASIA"), ("RUSSIA", "EUROPE"), ("UNITED KINGDOM", "EUROPE"),
    ("UNITED STATES", "AMERICA"),
]

SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]

ORDER_PRIORITY = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
ORDER_STATUS = ["F", "O", "P"]

SHIP_INSTRUCT = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
SHIP_MODE = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]

PART_TYPES_W1 = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
PART_TYPES_W2 = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
PART_TYPES_W3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]

CONTAINERS_W1 = ["SM", "MED", "LG", "JUMBO", "WRAP"]
CONTAINERS_W2 = ["CASE", "BOX", "PACK", "PKG", "JAR", "CAN", "DRUM", "BAG"]

RETURN_FLAG = ["R", "A", "N"]
LINE_STATUS = ["F", "O"]


class TPCHGenerator:
    """Generate TPC-H derived datasets at various scale factors."""

    def __init__(self, scale_factor: int = 1, seed: int = 42):
        self.sf = scale_factor
        self.rng = np.random.default_rng(seed)

        # Row counts per TPC-H specification
        self.n_lineitem = self.sf * 6_000_000     # Approximate, depends on orders
        self.n_orders = self.sf * 1_500_000
        self.n_customer = self.sf * 150_000
        self.n_part = self.sf * 200_000
        self.n_partsupp = self.sf * 800_000
        self.n_supplier = self.sf * 10_000

    def generate_region(self) -> pa.Table:
        """Generate the region table (5 rows, fixed)."""
        return pa.table({
            "r_regionkey": pa.array(range(5), type=pa.int32()),
            "r_name": pa.array(REGIONS),
            "r_comment": pa.array([f"Region comment for {r}" for r in REGIONS]),
        })

    def generate_nation(self) -> pa.Table:
        """Generate the nation table (25 rows, fixed)."""
        region_map = {r: i for i, r in enumerate(REGIONS)}
        return pa.table({
            "n_nationkey": pa.array(range(25), type=pa.int32()),
            "n_name": pa.array([n[0] for n in NATIONS]),
            "n_regionkey": pa.array([region_map[n[1]] for n in NATIONS],
                                    type=pa.int32()),
            "n_comment": pa.array([f"Nation comment for {n[0]}" for n in NATIONS]),
        })

    def generate_supplier(self) -> pa.Table:
        """Generate the supplier table."""
        n = self.n_supplier
        rng = self.rng
        return pa.table({
            "s_suppkey": pa.array(np.arange(1, n + 1), type=pa.int32()),
            "s_name": pa.array([f"Supplier#{i:09d}" for i in range(1, n + 1)]),
            "s_address": pa.array([f"{rng.integers(1,9999)} Supplier Street"
                                   for _ in range(n)]),
            "s_nationkey": pa.array(rng.integers(0, 25, size=n), type=pa.int32()),
            "s_phone": pa.array([
                f"{rng.integers(10,34):02d}-{rng.integers(100,999)}-"
                f"{rng.integers(100,999)}-{rng.integers(1000,9999)}"
                for _ in range(n)
            ]),
            "s_acctbal": pa.array(
                np.round(rng.uniform(-999.99, 9999.99, size=n), 2),
                type=pa.float64()
            ),
            "s_comment": pa.array([f"Supplier {i} comment" for i in range(1, n + 1)]),
        })

    def generate_customer(self) -> pa.Table:
        """Generate the customer table."""
        n = self.n_customer
        rng = self.rng
        return pa.table({
            "c_custkey": pa.array(np.arange(1, n + 1), type=pa.int32()),
            "c_name": pa.array([f"Customer#{i:018d}" for i in range(1, n + 1)]),
            "c_address": pa.array([f"{rng.integers(1,9999)} Customer Ave"
                                   for _ in range(n)]),
            "c_nationkey": pa.array(rng.integers(0, 25, size=n), type=pa.int32()),
            "c_phone": pa.array([
                f"{rng.integers(10,34):02d}-{rng.integers(100,999)}-"
                f"{rng.integers(100,999)}-{rng.integers(1000,9999)}"
                for _ in range(n)
            ]),
            "c_acctbal": pa.array(
                np.round(rng.uniform(-999.99, 9999.99, size=n), 2),
                type=pa.float64()
            ),
            "c_mktsegment": pa.array(rng.choice(SEGMENTS, size=n)),
            "c_comment": pa.array([f"Customer {i} comment" for i in range(1, n + 1)]),
        })

    def generate_part(self) -> pa.Table:
        """Generate the part table."""
        n = self.n_part
        rng = self.rng

        types = [f"{w1} {w2} {w3}" for w1 in PART_TYPES_W1
                 for w2 in PART_TYPES_W2 for w3 in PART_TYPES_W3]
        containers = [f"{w1} {w2}" for w1 in CONTAINERS_W1 for w2 in CONTAINERS_W2]

        return pa.table({
            "p_partkey": pa.array(np.arange(1, n + 1), type=pa.int32()),
            "p_name": pa.array([f"Part {i}" for i in range(1, n + 1)]),
            "p_mfgr": pa.array([f"Manufacturer#{rng.integers(1,6)}" for _ in range(n)]),
            "p_brand": pa.array([f"Brand#{rng.integers(1,6)}{rng.integers(1,6)}"
                                 for _ in range(n)]),
            "p_type": pa.array(rng.choice(types, size=n)),
            "p_size": pa.array(rng.integers(1, 51, size=n), type=pa.int32()),
            "p_container": pa.array(rng.choice(containers, size=n)),
            "p_retailprice": pa.array(
                np.round(rng.uniform(900.00, 2100.00, size=n), 2),
                type=pa.float64()
            ),
            "p_comment": pa.array([f"Part {i} comment" for i in range(1, n + 1)]),
        })

    def generate_partsupp(self) -> pa.Table:
        """Generate the partsupp table (4 suppliers per part)."""
        n = self.n_partsupp
        rng = self.rng

        # Each part has 4 suppliers
        ps_partkey = np.repeat(np.arange(1, self.n_part + 1), 4)[:n]
        ps_suppkey = np.tile(
            rng.permutation(np.arange(1, self.n_supplier + 1))[:4],
            self.n_part
        )[:n]

        # Ensure valid supplier keys
        ps_suppkey = rng.integers(1, self.n_supplier + 1, size=n)

        return pa.table({
            "ps_partkey": pa.array(ps_partkey, type=pa.int32()),
            "ps_suppkey": pa.array(ps_suppkey, type=pa.int32()),
            "ps_availqty": pa.array(rng.integers(1, 9999, size=n), type=pa.int32()),
            "ps_supplycost": pa.array(
                np.round(rng.uniform(1.00, 1000.00, size=n), 2),
                type=pa.float64()
            ),
            "ps_comment": pa.array([f"PartSupp {i}" for i in range(n)]),
        })

    def generate_orders(self) -> pa.Table:
        """Generate the orders table."""
        n = self.n_orders
        rng = self.rng

        # Order dates: 1992-01-01 to 1998-08-02 (as strings)
        base_days = rng.integers(0, 2406, size=n)  # ~6.6 years in days
        dates = []
        for d in base_days:
            year = 1992 + d // 365
            remainder = d % 365
            month = remainder // 30 + 1
            day = remainder % 30 + 1
            if year > 1998:
                year = 1998
            if month > 12:
                month = 12
            if day > 28:
                day = 28
            dates.append(f"{year}-{month:02d}-{day:02d}")

        return pa.table({
            "o_orderkey": pa.array(np.arange(1, n + 1), type=pa.int64()),
            "o_custkey": pa.array(
                rng.integers(1, self.n_customer + 1, size=n), type=pa.int32()
            ),
            "o_orderstatus": pa.array(rng.choice(ORDER_STATUS, size=n)),
            "o_totalprice": pa.array(
                np.round(rng.uniform(800.00, 600000.00, size=n), 2),
                type=pa.float64()
            ),
            "o_orderdate": pa.array(dates),
            "o_orderpriority": pa.array(rng.choice(ORDER_PRIORITY, size=n)),
            "o_clerk": pa.array([f"Clerk#{rng.integers(1, self.sf * 1000 + 1):09d}"
                                 for _ in range(n)]),
            "o_shippriority": pa.array(np.zeros(n, dtype=np.int32)),
            "o_comment": pa.array([f"Order {i}" for i in range(1, n + 1)]),
        })

    def generate_lineitem(self, orders: pa.Table) -> pa.Table:
        """Generate the lineitem fact table.

        Generated in chunks to manage memory on Pi hardware.
        """
        n = self.n_lineitem
        rng = self.rng
        chunk_size = min(1_000_000, n)

        orderkeys = orders.column("o_orderkey").to_numpy()
        orderdates = orders.column("o_orderdate").to_pylist()
        orderkey_to_date = dict(zip(orderkeys, orderdates))

        chunks = []
        rows_generated = 0

        print(f"  Generating lineitem: {n:,} rows in "
              f"{(n + chunk_size - 1) // chunk_size} chunks...")

        while rows_generated < n:
            current_chunk = min(chunk_size, n - rows_generated)

            l_orderkey = rng.choice(orderkeys, size=current_chunk)
            l_linenumber = rng.integers(1, 8, size=current_chunk).astype(np.int32)
            l_quantity = rng.integers(1, 51, size=current_chunk).astype(np.int32)

            l_extendedprice = np.round(
                rng.uniform(900.00, 105000.00, size=current_chunk), 2
            )
            l_discount = np.round(rng.uniform(0.00, 0.10, size=current_chunk), 2)
            l_tax = np.round(rng.uniform(0.00, 0.08, size=current_chunk), 2)

            chunk = pa.table({
                "l_orderkey": pa.array(l_orderkey, type=pa.int64()),
                "l_partkey": pa.array(
                    rng.integers(1, self.n_part + 1, size=current_chunk),
                    type=pa.int32()
                ),
                "l_suppkey": pa.array(
                    rng.integers(1, self.n_supplier + 1, size=current_chunk),
                    type=pa.int32()
                ),
                "l_linenumber": pa.array(l_linenumber),
                "l_quantity": pa.array(l_quantity),
                "l_extendedprice": pa.array(l_extendedprice, type=pa.float64()),
                "l_discount": pa.array(l_discount, type=pa.float64()),
                "l_tax": pa.array(l_tax, type=pa.float64()),
                "l_returnflag": pa.array(rng.choice(RETURN_FLAG, size=current_chunk)),
                "l_linestatus": pa.array(rng.choice(LINE_STATUS, size=current_chunk)),
                "l_shipdate": pa.array(rng.choice(orderdates, size=current_chunk)),
                "l_commitdate": pa.array(rng.choice(orderdates, size=current_chunk)),
                "l_receiptdate": pa.array(rng.choice(orderdates, size=current_chunk)),
                "l_shipinstruct": pa.array(
                    rng.choice(SHIP_INSTRUCT, size=current_chunk)
                ),
                "l_shipmode": pa.array(rng.choice(SHIP_MODE, size=current_chunk)),
                "l_comment": pa.array([f"lineitem {rows_generated + i}"
                                       for i in range(current_chunk)]),
            })
            chunks.append(chunk)
            rows_generated += current_chunk
            print(f"    {rows_generated:,}/{n:,} rows ({100*rows_generated/n:.0f}%)")

        return pa.concat_tables(chunks)

    def generate_all(self, output_dir: str, fmt: str = "parquet",
                     compression: str = "snappy",
                     row_group_size: int = 100_000) -> dict:
        """Generate all TPC-H tables and write to disk."""
        base = os.path.join(output_dir, "tpch", f"sf{self.sf}", fmt)
        os.makedirs(base, exist_ok=True)
        results = {}

        print(f"\n{'='*60}")
        print(f"TPC-H Derived Dataset Generation — Scale Factor {self.sf}")
        print(f"{'='*60}")

        print("\nGenerating fixed tables...")
        for name, gen_fn in [("region", self.generate_region),
                              ("nation", self.generate_nation)]:
            print(f"  {name}...")
            table = gen_fn()
            results[name] = write_table(
                table, os.path.join(base, name), fmt, compression, row_group_size
            )

        print("\nGenerating dimensions...")
        for name, gen_fn in [("supplier", self.generate_supplier),
                              ("customer", self.generate_customer),
                              ("part", self.generate_part),
                              ("partsupp", self.generate_partsupp)]:
            print(f"  {name}...")
            table = gen_fn()
            results[name] = write_table(
                table, os.path.join(base, name), fmt, compression, row_group_size
            )

        print("\nGenerating orders...")
        orders = self.generate_orders()
        results["orders"] = write_table(
            orders, os.path.join(base, "orders"), fmt, compression, row_group_size
        )

        print("\nGenerating lineitem (fact table)...")
        lineitem = self.generate_lineitem(orders)
        results["lineitem"] = write_table(
            lineitem, os.path.join(base, "lineitem"), fmt, compression, row_group_size
        )

        print_generation_summary(results)
        return results
