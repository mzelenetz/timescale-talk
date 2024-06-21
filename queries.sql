-- SETUP TABLES
create table stock_prices (
time timestamp not NULL,
symbol text not null,
price numeric,
day_volume numeric
);

create table company (
symbol text not null,
name text
);


select * from company  sp 

select create_hypertable('stock_prices', 
	by_range('time', interval '1 day') -- defaults to 7 day chunks
	);



-- INSERT DATA FOR DEMO:
--docker exec -it --user postgres a6a396860fb3 psql -U timescaledb -W postgres
--\COPY stock_prices from 'tutorial_sample_tick.csv' DELIMITER ',' CSV HEADER;
--\COPY company from 'tutorial_sample_company.csv' DELIMITER ',' CSV HEADER;

CREATE INDEX ix_stock_prices_symbol_time ON stock_prices (symbol, time DESC);
CREATE INDEX ix_company_symbol ON company (symbol);

--
select count(*)
from stock_prices;

select count(*)
from company ;

select * from company c 
--

select *
from stock_prices sp 
join company c on sp.symbol = c.symbol 
--where symbol = 'AAPL'
limit 100
order by time


-- SEE HOW TIMESCALE BREAKS UP THE TABLE
select * 
from _timescaledb_catalog.hypertable h 

-- EXAMPLE PEEKING INSIDE A CHUNK
select *
from _timescaledb_internal."_hyper_18_61_chunk" hc 

-- You may look at this
select * from timescaledb_information.hypertables hcs 

select * from timescaledb_information.chunks c 

  
 
-- CHECK SIZE
SELECT pg_size_pretty( pg_total_relation_size('stocks') );

-- Compress
ALTER TABLE stock_prices 
SET (timescaledb.compress = true, 
	timescaledb.compress_orderby='time',
	timescaledb.compress_segmentby='symbol'); 

-- Compress a specific chunk

SELECT pg_size_pretty( pg_total_relation_size('_timescaledb_internal._hyper_18_60_chunk') );

SELECT compress_chunk('_timescaledb_internal._hyper_18_60_chunk');

-- Add a policy

SELECT add_compression_policy('stock_prices',
							  compress_after => INTERVAL '1d');
							 
							 
-- CHECK SIZE
SELECT pg_size_pretty( pg_total_relation_size('stock_prices') );

-- SEE settings
select * from timescaledb_information.compression_settings cs 

-- Some useful tools
select * from _timescaledb_catalog.hypertable  h ;
select * from hypertable_compression_stats('stock_prices'); 

select * from _timescaledb_catalog.compression_algorithm ca 
-- Drop underlying data after 6 Months

SELECT add_retention_policy('stock_prices', 
					drop_after => INTERVAL '6 months');

-- Using some sample built in functions
select 
	time_bucket(interval '10 min', time) as ts,
	symbol, 
	avg(price) as avg_price, 
	max(price) as max_price, 
	min(price) as min_price,
	first(price, time) as first_price
from stock_prices sp 
where symbol = 'AAPL'
group by ts, symbol


select *
from stock_prices sp 
-- CREATE CAGG
--drop materialized view cagg_ohlc_1min;

create materialized view cagg_ohlc_1min2 with (timescaledb.continuous) as 
select 
	time_bucket(interval '1 minutes', time) as ts,
	symbol, 
	max(price) as high, 
	min(price) as low,
	first(price, time) as open, 
	last(price, time) as close
from stock_prices sp 
group by ts, symbol
with no data;

-- CREATE POLICY
SELECT add_continuous_aggregate_policy('cagg_ohlc_1min2',
  start_offset => INTERVAL '1 month',
  end_offset => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 hour');

-- CREATE CAGG ON CAGG
create materialized view cagg_ohlc_1hour with (timescaledb.continuous) as 
select 
	time_bucket(interval '10 hour', ts) as ts,
	symbol, 
	max(high) as high, 
	min(low) as low,
	first(open, ts) as open, 
	last(close, ts) as close
from cagg_ohlc_1min sp 
group by ts, symbol


 
 -- QUERY RELATIONAL TABLE
 
 select * 
 from cagg_ohlc_1min m
 join company c on c.symbol = m.symbol  
 --Add compression
create materialized view cagg_ohlc_1hour with (timescaledb.continuous) as 
select 
	time_bucket(interval '10 hour', ts) as ts,
	symbol, 
	max(high) as high, 
	min(low) as low,
	first(open, ts) as open, 
	last(close, ts) as close
from cagg_ohlc_1min sp 
group by ts, symbol
