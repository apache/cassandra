SOURCE 'keyspace-create-simple.cql';
SOURCE 'keyspace-use.cql';

// tag::drop[]
DROP TABLE IF EXISTS cycling.comments;
// end::drop[]

/* Support fast retrieval of first record on each partition
   and show upserts of timestamp type fields */

// tag::comments-table[]
CREATE TABLE IF NOT EXISTS cycling.comments (
  record_id timeuuid,
  id uuid,
  commenter text,
  comment text,
  created_at timestamp,
  PRIMARY KEY (id, created_at)
)
WITH CLUSTERING ORDER BY (created_at DESC);
// end::comments-table[]

DROP INDEX IF EXISTS cycling.fn_sparse;

// tag::fn_sparse[]
CREATE CUSTOM INDEX IF NOT EXISTS fn_sparse
ON cycling.comments (created_at)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = { 'mode': 'SPARSE' };
// end::fn_sparse[]

// tag::insertall[]
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), e7ae5cf3-d358-4d99-b900-85902fda9bb0, '2017-02-14 12:43:20-0800', 'Raining too hard should have postponed', 'Alex');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), e7ae5cf3-d358-4d99-b900-85902fda9bb0, '2017-02-14 12:43:20.234-0800', 'Raining too hard should have postponed', 'Alex');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), e7ae5cf3-d358-4d99-b900-85902fda9bb0, '2017-03-21 13:11:09.999-0800', 'Second rest stop was out of water', 'Alex');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), e7ae5cf3-d358-4d99-b900-85902fda9bb0, '2017-04-01 06:33:02.16-0800', 'LATE RIDERS SHOULD NOT DELAY THE START', 'Alex');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), c7fceba0-c141-4207-9494-a29f9809de6f, totimestamp(now()), 'The gift certificate for winning was the best', 'Amy');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), c7fceba0-c141-4207-9494-a29f9809de6f, '2017-02-17 12:43:20.234+0400', 'Glad you ran the race in the rain', 'Amy');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), c7fceba0-c141-4207-9494-a29f9809de6f, '2017-03-22 5:16:59.001+0400', 'Great snacks at all reststops', 'Amy');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), c7fceba0-c141-4207-9494-a29f9809de6f, '2017-04-01 17:43:08.030+0400', 'Last climb was a killer', 'Amy');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 8566eb59-07df-43b1-a21b-666a3c08c08a, totimestamp(now()), 'Fastest womens time ever way to go amy!', 'Maryanne');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 8566eb59-07df-43b1-a21b-666a3c08c08a, '2017-02-13 11:20:17.020-0600', 'Great race on a crappy day', 'Maryanne');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 8566eb59-07df-43b1-a21b-666a3c08c08a, '2017-03-20 15:45:10.101-0600', 'Saggers really rocked it', 'Maryanne');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 8566eb59-07df-43b1-a21b-666a3c08c08a, '2017-04-14 05:16:52.009-0600', 'Not bad for a flatlander', 'Maryanne');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), fb372533-eb95-4bb4-8685-6ef61e994caa, totimestamp(now()), 'Great course', 'Michael');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), fb372533-eb95-4bb4-8685-6ef61e994caa, '2017-02-15 18:22:11-0800', 'Some entries complain a lot', 'Michael');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), fb372533-eb95-4bb4-8685-6ef61e994caa, '2017-03-16 19:43:01.030-0800', 'Getting read for the race', 'Michael');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), fb372533-eb95-4bb4-8685-6ef61e994caa, '2017-03-22 1:19:44.060-0800', 'Awesome race glad you held it anyway', 'Michael');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), fb372533-eb95-4bb4-8685-6ef61e994caa, '2017-04-07 11:21:14.001-0800', 'Thanks for waiting for me!', 'Michael');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 9011d3be-d35c-4a8d-83f7-a3c543789ee7, totimestamp(now()), 'Can''t wait for the next race', 'Katarzyna');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 9011d3be-d35c-4a8d-83f7-a3c543789ee7, '2017-01-01 11:20:17.020-0600', 'Gearing up for the seaon', 'Katarzyna');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 5b6962dd-3f90-4c93-8f61-eabfa4a803e2, totimestamp(now()), 'Thanks for all your hard work', 'Marianne');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 220844bf-4860-49d6-9a4b-6b5d3a79cbfb, totimestamp(now()), 'A for effort!', 'Paolo');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), c4b65263-fe58-4846-83e8-f0e1c13d518f, totimestamp(now()), 'Closing ceremony was a little lame', 'Rossella');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 38ab64b6-26cc-4de9-ab28-c257cf011659, totimestamp(now()), 'Next time guys!', 'Marcia');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 38ab64b6-26cc-4de9-ab28-c257cf011659, '2017-02-11 22:09:56+0800', 'First race was amazing, can''t wait for more', 'Marcia');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, totimestamp(now()), 'So many great races thanks y''all', 'Steven');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, '2017-02-02 09:49:00.02+0800', 'Best of luck everybody I can''t make it', 'Steven');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, '2017-04-05 12:01:00.003', 'Bike damaged in transit bummer', 'Steven');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), e7cd5752-bc0d-4157-a80f-7523add8dbcd, totimestamp(now()), 'Go team, you rocked it', 'Anna');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 6d5f1663-89c0-45fc-8cfd-60a373b01622, totimestamp(now()), 'Next year the tour of california!', 'Melissa');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 95addc4c-459e-4ed7-b4b5-472f19a67995, totimestamp(now()), 'Next year for sure!', 'Vera');
INSERT INTO cycling.comments (record_id, id, created_at, comment, commenter) VALUES (now(), 95addc4c-459e-4ed7-b4b5-472f19a67995, '2017-02-13 11:40:16.123-0600', 'I can do without the rain@@@@', 'Vera');
// end::insertall[]

CAPTURE 'select_from_comments.results';
// tag::select[]
SELECT *
FROM cycling.comments
WHERE created_at < '2017-02-13 11:40:16.123';
// end::select[]
CAPTURE OFF;

// tag::alt[]
ALTER TABLE cycling.comments
WITH compression = {
  'sstable_compression' : 'DeflateCompressor',
  'chunk_length_kb' : 64
};
// end::alt[]

// tag::cache[]
ALTER TABLE cycling.comments
WITH caching = {
  'keys' : 'NONE',
  'rows_per_partition' : 10
};
// end::cache[]

// tag::nocompact[]
ALTER TABLE cycling.comments
WITH COMPACTION = {
  'class' : 'SizeTieredCompactionStrategy',
  'enabled' : 'false'
};
// end::nocompact[]

// tag::compaction[]
ALTER TABLE cycling.comments
WITH COMPACTION = {
  'class' : 'SizeTieredCompactionStrategy',
  'enabled' : 'true'
};
// end::compaction[]