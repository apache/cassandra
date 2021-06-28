CREATE TABLE simple (
id int,
key text,
value text,
PRIMARY KEY (key, value)
) WITH caching = {'keys': 'ALL', 'rows_per_partition': 10};
