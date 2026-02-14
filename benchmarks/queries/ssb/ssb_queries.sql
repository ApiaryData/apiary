-- ============================================================
-- Star Schema Benchmark (SSB) Queries for Apiary
-- 13 queries in 4 flights with increasing selectivity
--
-- Reference: O'Neil, O'Neil, Chen - "Star Schema Benchmark" (2009)
--
-- Naming: Q{flight}.{query} e.g., Q1.1, Q2.3
-- Tables: lineorder (fact), customer, supplier, part, date (dims)
-- ============================================================

-- ============================================================
-- FLIGHT 1: Revenue by date filtering
-- Tests: scan + filter + aggregate on fact table with date join
-- Selectivity increases from Q1.1 to Q1.3
-- ============================================================

-- Q1.1: Revenue for a specific year with discount/quantity filter
-- Selectivity: ~1.5% of lineorder
SELECT SUM(lo_extendedprice * lo_discount) AS revenue
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
WHERE d_year = 1993
  AND lo_discount BETWEEN 1 AND 3
  AND lo_quantity < 25;

-- Q1.2: Revenue for a specific year-month with discount/quantity filter
-- Selectivity: ~0.13% of lineorder
SELECT SUM(lo_extendedprice * lo_discount) AS revenue
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
WHERE d_yearmonthnum = 199401
  AND lo_discount BETWEEN 4 AND 6
  AND lo_quantity BETWEEN 26 AND 35;

-- Q1.3: Revenue for a specific week with discount/quantity filter
-- Selectivity: ~0.02% of lineorder
SELECT SUM(lo_extendedprice * lo_discount) AS revenue
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
WHERE d_weeknuminyear = 6
  AND d_year = 1994
  AND lo_discount BETWEEN 5 AND 7
  AND lo_quantity BETWEEN 26 AND 35;


-- ============================================================
-- FLIGHT 2: Revenue by product brand grouped by year
-- Tests: 2-way join (part + date), group by, order by
-- Selectivity increases through region/category/brand filtering
-- ============================================================

-- Q2.1: Revenue by brand for a region, all years
SELECT SUM(lo_revenue) AS lo_revenue, d_year, p_brand
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
JOIN part ON lo_partkey = p_partkey
JOIN supplier ON lo_suppkey = s_suppkey
WHERE p_category = 'MFGR#12'
  AND s_region = 'AMERICA'
GROUP BY d_year, p_brand
ORDER BY d_year, p_brand;

-- Q2.2: Revenue by brand for a region, specific brands
SELECT SUM(lo_revenue) AS lo_revenue, d_year, p_brand
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
JOIN part ON lo_partkey = p_partkey
JOIN supplier ON lo_suppkey = s_suppkey
WHERE p_brand BETWEEN 'MFGR#2221' AND 'MFGR#2228'
  AND s_region = 'ASIA'
GROUP BY d_year, p_brand
ORDER BY d_year, p_brand;

-- Q2.3: Revenue by brand for a specific brand
SELECT SUM(lo_revenue) AS lo_revenue, d_year, p_brand
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
JOIN part ON lo_partkey = p_partkey
JOIN supplier ON lo_suppkey = s_suppkey
WHERE p_brand = 'MFGR#2239'
  AND s_region = 'EUROPE'
GROUP BY d_year, p_brand
ORDER BY d_year, p_brand;


-- ============================================================
-- FLIGHT 3: Revenue by customer/supplier nation grouped by year
-- Tests: 3-way join (customer + supplier + date), geographic filtering
-- Selectivity narrows from region to nation to city
-- ============================================================

-- Q3.1: Revenue by customer/supplier nation in ASIA
SELECT c_nation, s_nation, d_year,
       SUM(lo_revenue) AS lo_revenue
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
JOIN customer ON lo_custkey = c_custkey
JOIN supplier ON lo_suppkey = s_suppkey
WHERE c_region = 'ASIA'
  AND s_region = 'ASIA'
  AND d_year >= 1992 AND d_year <= 1997
GROUP BY c_nation, s_nation, d_year
ORDER BY d_year ASC, lo_revenue DESC;

-- Q3.2: Revenue by customer/supplier nation (specific nations)
SELECT c_city, s_city, d_year,
       SUM(lo_revenue) AS lo_revenue
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
JOIN customer ON lo_custkey = c_custkey
JOIN supplier ON lo_suppkey = s_suppkey
WHERE c_nation = 'UNITED STATES'
  AND s_nation = 'UNITED STATES'
  AND d_year >= 1992 AND d_year <= 1997
GROUP BY c_city, s_city, d_year
ORDER BY d_year ASC, lo_revenue DESC;

-- Q3.3: Revenue by city pair (specific cities)
SELECT c_city, s_city, d_year,
       SUM(lo_revenue) AS lo_revenue
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
JOIN customer ON lo_custkey = c_custkey
JOIN supplier ON lo_suppkey = s_suppkey
WHERE (c_city = 'UNITED KI1' OR c_city = 'UNITED KI5')
  AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5')
  AND d_year >= 1992 AND d_year <= 1997
GROUP BY c_city, s_city, d_year
ORDER BY d_year ASC, lo_revenue DESC;

-- Q3.4: Revenue by city pair in specific month
SELECT c_city, s_city, d_year,
       SUM(lo_revenue) AS lo_revenue
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
JOIN customer ON lo_custkey = c_custkey
JOIN supplier ON lo_suppkey = s_suppkey
WHERE (c_city = 'UNITED KI1' OR c_city = 'UNITED KI5')
  AND (s_city = 'UNITED KI1' OR s_city = 'UNITED KI5')
  AND d_yearmonth = 'Dec1997'
GROUP BY c_city, s_city, d_year
ORDER BY d_year ASC, lo_revenue DESC;


-- ============================================================
-- FLIGHT 4: Profit by year and customer nation
-- Tests: 4-way join (all dimensions), profit computation
-- Most complex queries in SSB
-- ============================================================

-- Q4.1: Profit by year and nation for specific region/manufacturer
SELECT d_year, c_nation,
       SUM(lo_revenue - lo_supplycost) AS profit
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
JOIN customer ON lo_custkey = c_custkey
JOIN supplier ON lo_suppkey = s_suppkey
JOIN part ON lo_partkey = p_partkey
WHERE c_region = 'AMERICA'
  AND s_region = 'AMERICA'
  AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2')
GROUP BY d_year, c_nation
ORDER BY d_year, c_nation;

-- Q4.2: Profit by year and nation/category
SELECT d_year, s_nation, p_category,
       SUM(lo_revenue - lo_supplycost) AS profit
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
JOIN customer ON lo_custkey = c_custkey
JOIN supplier ON lo_suppkey = s_suppkey
JOIN part ON lo_partkey = p_partkey
WHERE c_region = 'AMERICA'
  AND s_region = 'AMERICA'
  AND (d_year = 1997 OR d_year = 1998)
  AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2')
GROUP BY d_year, s_nation, p_category
ORDER BY d_year, s_nation, p_category;

-- Q4.3: Profit by year, city, and brand (most selective)
SELECT d_year, s_city, p_brand,
       SUM(lo_revenue - lo_supplycost) AS profit
FROM lineorder
JOIN date ON lo_orderdate = d_datekey
JOIN customer ON lo_custkey = c_custkey
JOIN supplier ON lo_suppkey = s_suppkey
JOIN part ON lo_partkey = p_partkey
WHERE c_region = 'AMERICA'
  AND s_nation = 'UNITED STATES'
  AND (d_year = 1997 OR d_year = 1998)
  AND p_category = 'MFGR#14'
GROUP BY d_year, s_city, p_brand
ORDER BY d_year, s_city, p_brand;
