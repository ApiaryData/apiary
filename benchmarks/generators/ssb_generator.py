"""
Star Schema Benchmark (SSB) Dataset Generator for Apiary.

Generates the 5-table star schema defined by Pat O'Neil's SSB specification:
  - lineorder (fact table): SF * 6,000,000 rows
  - customer (dimension): SF * 30,000 rows
  - supplier (dimension): SF * 2,000 rows
  - part (dimension): 200,000 * (1 + log2(SF)) rows
  - date (dimension): 2,556 rows (fixed, 7 years * 12 months * ~28 days)

Reference: "Star Schema Benchmark" by Pat O'Neil, Betty O'Neil, Xuedong Chen (2009)
"""

import os
import numpy as np
import pyarrow as pa
from .utils import write_table, generate_date_dimension, print_generation_summary


# --- Domain value lists (matching SSB spec) ---

REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]

NATIONS_BY_REGION = {
    "AFRICA": ["ALGERIA", "ETHIOPIA", "KENYA", "MOROCCO", "MOZAMBIQUE"],
    "AMERICA": ["ARGENTINA", "BRAZIL", "CANADA", "PERU", "UNITED STATES"],
    "ASIA": ["CHINA", "INDIA", "INDONESIA", "JAPAN", "VIETNAM"],
    "EUROPE": ["FRANCE", "GERMANY", "ROMANIA", "RUSSIA", "UNITED KINGDOM"],
    "MIDDLE EAST": ["EGYPT", "IRAN", "IRAQ", "JORDAN", "SAUDI ARABIA"],
}
ALL_NATIONS = [n for ns in NATIONS_BY_REGION.values() for n in ns]
NATION_TO_REGION = {n: r for r, ns in NATIONS_BY_REGION.items() for n in ns}

CITIES_PER_NATION = 10  # SSB generates numbered cities: "UNITED ST0", "UNITED ST1", etc.

MFGR_COUNT = 5          # MFGR#1 through MFGR#5
CATEGORY_PER_MFGR = 5   # Each MFGR has 5 categories
BRAND_PER_CATEGORY = 40  # Each category has 40 brands

CONTAINERS = ["SM CASE", "SM BOX", "SM PACK", "SM PKG", "SM JAR", "SM DRUM",
              "MED CASE", "MED BOX", "MED PACK", "MED PKG", "MED JAR", "MED DRUM",
              "LG CASE", "LG BOX", "LG PACK", "LG PKG", "LG JAR", "LG DRUM",
              "JUMBO CASE", "JUMBO BOX", "JUMBO PACK", "JUMBO PKG", "JUMBO JAR",
              "JUMBO DRUM", "WRAP CASE", "WRAP BOX", "WRAP PACK", "WRAP PKG",
              "WRAP JAR", "WRAP DRUM"]

TYPES_PREFIX = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
TYPES_SUFFIX = ["ANODIZED TIN", "ANODIZED NICKEL", "ANODIZED BRASS",
                "ANODIZED STEEL", "ANODIZED COPPER", "BURNISHED TIN",
                "BURNISHED NICKEL", "BURNISHED BRASS", "BURNISHED STEEL",
                "BURNISHED COPPER", "PLATED TIN", "PLATED NICKEL",
                "PLATED BRASS", "PLATED STEEL", "PLATED COPPER",
                "POLISHED TIN", "POLISHED NICKEL", "POLISHED BRASS",
                "POLISHED STEEL", "POLISHED COPPER", "BRUSHED TIN",
                "BRUSHED NICKEL", "BRUSHED BRASS", "BRUSHED STEEL",
                "BRUSHED COPPER"]

SHIPMODE = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
PRIORITY = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]


def _generate_cities(nations: list[str], count_per_nation: int = 10) -> list[str]:
    """Generate city names: nation prefix + digit (e.g., 'UNITED ST0')."""
    cities = []
    for nation in nations:
        prefix = nation[:9] if len(nation) > 9 else nation
        for i in range(count_per_nation):
            cities.append(f"{prefix}{i}")
    return cities


ALL_CITIES = _generate_cities(ALL_NATIONS, CITIES_PER_NATION)
CITY_TO_NATION = {}
for nation in ALL_NATIONS:
    prefix = nation[:9] if len(nation) > 9 else nation
    for i in range(CITIES_PER_NATION):
        CITY_TO_NATION[f"{prefix}{i}"] = nation


class SSBGenerator:
    """Generate Star Schema Benchmark datasets at various scale factors."""

    def __init__(self, scale_factor: int = 1, seed: int = 42):
        self.sf = scale_factor
        self.rng = np.random.default_rng(seed)

        # Row counts per SSB specification
        self.n_lineorder = self.sf * 6_000_000
        self.n_customer = self.sf * 30_000
        self.n_supplier = self.sf * 2_000
        self.n_part = int(200_000 * (1 + np.log2(max(self.sf, 1))))

    def generate_customer(self) -> pa.Table:
        """Generate the customer dimension table."""
        n = self.n_customer
        rng = self.rng

        cities = np.array(ALL_CITIES)
        c_city = cities[rng.integers(0, len(cities), size=n)]
        c_nation = np.array([CITY_TO_NATION[c] for c in c_city])
        c_region = np.array([NATION_TO_REGION[n] for n in c_nation])

        return pa.table({
            "c_custkey": pa.array(np.arange(1, n + 1), type=pa.int32()),
            "c_name": pa.array([f"Customer#{i:09d}" for i in range(1, n + 1)]),
            "c_address": pa.array([f"{rng.integers(1,9999)} Street {i}" for i in range(n)]),
            "c_city": pa.array(c_city),
            "c_nation": pa.array(c_nation),
            "c_region": pa.array(c_region),
            "c_phone": pa.array([f"{rng.integers(10,99)}-{rng.integers(100,999)}-"
                                 f"{rng.integers(100,999)}-{rng.integers(1000,9999)}"
                                 for _ in range(n)]),
            "c_mktsegment": pa.array(
                rng.choice(["AUTOMOBILE", "BUILDING", "FURNITURE",
                            "HOUSEHOLD", "MACHINERY"], size=n)
            ),
        })

    def generate_supplier(self) -> pa.Table:
        """Generate the supplier dimension table."""
        n = self.n_supplier
        rng = self.rng

        cities = np.array(ALL_CITIES)
        s_city = cities[rng.integers(0, len(cities), size=n)]
        s_nation = np.array([CITY_TO_NATION[c] for c in s_city])
        s_region = np.array([NATION_TO_REGION[n_] for n_ in s_nation])

        return pa.table({
            "s_suppkey": pa.array(np.arange(1, n + 1), type=pa.int32()),
            "s_name": pa.array([f"Supplier#{i:09d}" for i in range(1, n + 1)]),
            "s_address": pa.array([f"{rng.integers(1,9999)} Supplier St {i}" for i in range(n)]),
            "s_city": pa.array(s_city),
            "s_nation": pa.array(s_nation),
            "s_region": pa.array(s_region),
            "s_phone": pa.array([f"{rng.integers(10,99)}-{rng.integers(100,999)}-"
                                 f"{rng.integers(100,999)}-{rng.integers(1000,9999)}"
                                 for _ in range(n)]),
        })

    def generate_part(self) -> pa.Table:
        """Generate the part dimension table."""
        n = self.n_part
        rng = self.rng

        # Generate hierarchical MFGR -> Category -> Brand
        mfgr_ids = rng.integers(1, MFGR_COUNT + 1, size=n)
        cat_ids = rng.integers(1, CATEGORY_PER_MFGR + 1, size=n)
        brand_ids = rng.integers(1, BRAND_PER_CATEGORY + 1, size=n)

        p_mfgr = np.array([f"MFGR#{m}" for m in mfgr_ids])
        p_category = np.array([f"MFGR#{m}{c}" for m, c in zip(mfgr_ids, cat_ids)])
        p_brand = np.array([f"MFGR#{m}{c}{b:02d}" for m, c, b in
                            zip(mfgr_ids, cat_ids, brand_ids)])

        types = [f"{p} {s}" for p in TYPES_PREFIX for s in TYPES_SUFFIX]

        return pa.table({
            "p_partkey": pa.array(np.arange(1, n + 1), type=pa.int32()),
            "p_name": pa.array([f"Part#{i:09d}" for i in range(1, n + 1)]),
            "p_mfgr": pa.array(p_mfgr),
            "p_category": pa.array(p_category),
            "p_brand": pa.array(p_brand),
            "p_color": pa.array(rng.choice(
                ["almond", "antique", "aquamarine", "azure", "beige", "bisque",
                 "black", "blanched", "blue", "blush", "brown", "burlywood",
                 "burnished", "chartreuse", "chiffon", "chocolate", "coral",
                 "cornflower", "cornsilk", "cream", "cyan", "dark", "deep",
                 "dim", "dodger", "drab", "firebrick", "floral", "forest",
                 "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey",
                 "honeydew", "hot", "indian", "ivory", "khaki", "lace",
                 "lavender", "lawn", "lemon", "light", "lime", "linen",
                 "magenta", "maroon", "medium", "metallic", "midnight", "mint",
                 "misty", "moccasin", "navajo", "navy", "olive", "orange",
                 "orchid", "pale", "papaya", "peach", "peru", "pink", "plum",
                 "powder", "puff", "purple", "red", "rose", "rosy", "royal",
                 "saddle", "salmon", "sandy", "seashell", "sienna", "sky",
                 "slate", "smoke", "snow", "spring", "steel", "tan", "thistle",
                 "tomato", "turquoise", "violet", "wheat", "white", "yellow"],
                size=n
            )),
            "p_type": pa.array(rng.choice(types, size=n)),
            "p_size": pa.array(rng.integers(1, 51, size=n), type=pa.int32()),
            "p_container": pa.array(rng.choice(CONTAINERS, size=n)),
        })

    def generate_date(self) -> pa.Table:
        """Generate the date dimension table (fixed, 7 years)."""
        return generate_date_dimension(start_year=1992, end_year=1998)

    def generate_lineorder(self, customer: pa.Table, supplier: pa.Table,
                           part: pa.Table, date: pa.Table) -> pa.Table:
        """Generate the lineorder fact table.

        This is the largest table. At SF1, it has 6M rows.
        Generated in chunks to manage memory on constrained hardware.
        """
        n = self.n_lineorder
        rng = self.rng
        chunk_size = min(1_000_000, n)

        # Extract valid keys from dimensions
        custkeys = customer.column("c_custkey").to_numpy()
        suppkeys = supplier.column("s_suppkey").to_numpy()
        partkeys = part.column("p_partkey").to_numpy()
        datekeys = date.column("d_datekey").to_numpy()

        chunks = []
        orderkey = 1
        rows_generated = 0

        print(f"  Generating lineorder: {n:,} rows in {(n + chunk_size - 1) // chunk_size} chunks...")

        while rows_generated < n:
            current_chunk = min(chunk_size, n - rows_generated)

            # Generate order keys (each order has 1-7 line items)
            linenumbers = rng.integers(1, 8, size=current_chunk)
            orderkeys = np.arange(orderkey, orderkey + current_chunk)
            orderkey += current_chunk

            lo_custkey = rng.choice(custkeys, size=current_chunk)
            lo_suppkey = rng.choice(suppkeys, size=current_chunk)
            lo_partkey = rng.choice(partkeys, size=current_chunk)
            lo_orderdate = rng.choice(datekeys, size=current_chunk)

            # Commit dates: 1-122 days after order date
            commit_offsets = rng.integers(1, 123, size=current_chunk)
            lo_commitdate = rng.choice(datekeys, size=current_chunk)

            lo_quantity = rng.integers(1, 51, size=current_chunk).astype(np.int32)
            lo_extendedprice = (rng.integers(90000, 20000001, size=current_chunk)
                                .astype(np.int64))
            lo_discount = rng.integers(0, 11, size=current_chunk).astype(np.int32)
            lo_tax = rng.integers(0, 9, size=current_chunk).astype(np.int32)
            lo_revenue = (lo_extendedprice *
                          (100 - lo_discount) // 100).astype(np.int64)
            lo_supplycost = (rng.integers(50000, 12500001, size=current_chunk)
                             .astype(np.int64))

            chunk = pa.table({
                "lo_orderkey": pa.array(orderkeys, type=pa.int64()),
                "lo_linenumber": pa.array(linenumbers, type=pa.int32()),
                "lo_custkey": pa.array(lo_custkey, type=pa.int32()),
                "lo_partkey": pa.array(lo_partkey, type=pa.int32()),
                "lo_suppkey": pa.array(lo_suppkey, type=pa.int32()),
                "lo_orderdate": pa.array(lo_orderdate, type=pa.int32()),
                "lo_orderpriority": pa.array(rng.choice(PRIORITY, size=current_chunk)),
                "lo_shippriority": pa.array(
                    rng.integers(0, 2, size=current_chunk), type=pa.int32()
                ),
                "lo_quantity": pa.array(lo_quantity),
                "lo_extendedprice": pa.array(lo_extendedprice),
                "lo_ordtotalprice": pa.array(
                    (lo_extendedprice * lo_quantity // 100).astype(np.int64)
                ),
                "lo_discount": pa.array(lo_discount),
                "lo_revenue": pa.array(lo_revenue),
                "lo_supplycost": pa.array(lo_supplycost),
                "lo_tax": pa.array(lo_tax),
                "lo_commitdate": pa.array(lo_commitdate, type=pa.int32()),
                "lo_shipmode": pa.array(rng.choice(SHIPMODE, size=current_chunk)),
            })
            chunks.append(chunk)
            rows_generated += current_chunk
            print(f"    {rows_generated:,}/{n:,} rows ({100*rows_generated/n:.0f}%)")

        return pa.concat_tables(chunks)

    def generate_all(self, output_dir: str, fmt: str = "parquet",
                     compression: str = "snappy",
                     row_group_size: int = 100_000) -> dict:
        """Generate all SSB tables and write to disk.

        Returns dict of {table_name: (filepath, size_mb)}.
        """
        base = os.path.join(output_dir, "ssb", f"sf{self.sf}", fmt)
        os.makedirs(base, exist_ok=True)
        results = {}

        print(f"\n{'='*60}")
        print(f"SSB Dataset Generation — Scale Factor {self.sf}")
        print(f"{'='*60}")

        print("\nGenerating dimensions...")

        print("  customer...")
        customer = self.generate_customer()
        results["customer"] = write_table(
            customer, os.path.join(base, "customer"), fmt, compression, row_group_size
        )

        print("  supplier...")
        supplier = self.generate_supplier()
        results["supplier"] = write_table(
            supplier, os.path.join(base, "supplier"), fmt, compression, row_group_size
        )

        print("  part...")
        part = self.generate_part()
        results["part"] = write_table(
            part, os.path.join(base, "part"), fmt, compression, row_group_size
        )

        print("  date...")
        date = self.generate_date()
        results["date"] = write_table(
            date, os.path.join(base, "date"), fmt, compression, row_group_size
        )

        print("\nGenerating fact table...")
        lineorder = self.generate_lineorder(customer, supplier, part, date)
        results["lineorder"] = write_table(
            lineorder, os.path.join(base, "lineorder"), fmt, compression, row_group_size
        )

        print_generation_summary(results)
        return results
