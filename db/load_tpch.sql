-- Run this from the repo root, e.g.
--   psql -U postgres -d "TPC-H" -f db/load_tpch.sql
--
-- Assumes you have already:
--   1) created tables with: db/dss.ddl
--   2) generated TPC-H data, converted *.tbl -> *.csv (still pipe-delimited)
--      and placed the files under: db/data/
--
-- Note: these files are pipe-delimited, despite the .csv extension.

\echo Loading TPC-H data from db/data/*.csv (pipe-delimited)

\copy region   from 'db/data/region.csv'   with (format csv, delimiter '|', null '');
\copy nation   from 'db/data/nation.csv'   with (format csv, delimiter '|', null '');
\copy supplier from 'db/data/supplier.csv' with (format csv, delimiter '|', null '');
\copy customer from 'db/data/customer.csv' with (format csv, delimiter '|', null '');
\copy part     from 'db/data/part.csv'     with (format csv, delimiter '|', null '');
\copy partsupp from 'db/data/partsupp.csv' with (format csv, delimiter '|', null '');
\copy orders   from 'db/data/orders.csv'   with (format csv, delimiter '|', null '');
\copy lineitem from 'db/data/lineitem.csv' with (format csv, delimiter '|', null '');

\echo Done.
