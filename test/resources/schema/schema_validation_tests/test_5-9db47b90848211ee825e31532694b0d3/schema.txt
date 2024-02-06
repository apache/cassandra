CREATE TABLE IF NOT EXISTS schema_validation_tests.test_5 (
	col_txt text PRIMARY KEY,
	col_ascii ascii,
	col_bigint bigint,
	col_blob blob,
	col_bool boolean,
	col_date date,
	col_dbl double,
	col_dec decimal,
	col_float float,
	col_inet inet,
	col_small smallint,
	col_time time,
	col_timestamp timestamp,
	col_timeuuid timeuuid,
	col_tinyint tinyint,
	col_varchar text,
	col_varint varint)
	WITH ID = 9db47b90-8482-11ee-825e-31532694b0d3
	AND additional_write_policy = '99PERCENTILE'
	AND bloom_filter_fp_chance = 0.01
	AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
	AND cdc = false
	AND comment = ''
	AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
	AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
	AND crc_check_chance = 1.0
	AND default_time_to_live = 0
	AND extensions = {}
	AND gc_grace_seconds = 864000
	AND max_index_interval = 2048
	AND memtable_flush_period_in_ms = 0
	AND min_index_interval = 128
	AND nodesync = {'enabled': 'true', 'incremental': 'true'}
	AND read_repair = 'BLOCKING'
	AND speculative_retry = '99PERCENTILE';
