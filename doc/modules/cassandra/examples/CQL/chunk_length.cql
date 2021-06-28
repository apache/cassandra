CREATE TABLE simple (
   id int,
   key text,
   value text,
   PRIMARY KEY (key, value)
) WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 4};
