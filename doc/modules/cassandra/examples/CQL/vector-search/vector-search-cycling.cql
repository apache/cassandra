// tag::create-vs-keyspace-cycling[]
CREATE KEYSPACE IF NOT EXISTS cycling 
   WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };
// end::create-vs-keyspace-cycling[]

// tag::use-vs-keyspace-cycling[]
USE cycling;
// end::use-vs-keyspace-cycling[]

// tag::create-vs-table[]
CREATE TABLE IF NOT EXISTS cycling.comments_vs (
  record_id timeuuid,
  id uuid,
  commenter text,
  comment text,
  comment_vector VECTOR <FLOAT, 5>,
  created_at timestamp,
  PRIMARY KEY (id, created_at)
)
WITH CLUSTERING ORDER BY (created_at DESC);
// end::create-vs-table[]

// tag::alter-vs-table[]
ALTER TABLE cycling.comments_vs
   ADD comment_vector VECTOR <FLOAT, 5>; <1>
// end::alter-vs-table[]

// tag::create-vs-index[]
CREATE INDEX IF NOT EXISTS ann_index 
  ON cycling.comments_vs(comment_vector) USING 'sai';
// end::create-vs-index[]

// tag::create-vs-index-with-option[]
CREATE INDEX IF NOT EXISTS ann_index
    ON vsearch.com(item_vector) USING 'sai'
WITH OPTIONS = { 'similarity_function': 'DOT_PRODUCT' };
// end::create-vs-index-with-option[]

// tag::insert-vector-data-cycling[]
INSERT INTO cycling.comments_vs (record_id, id, created_at, comment, commenter, comment_vector)
   VALUES (
      now(), 
      e7ae5cf3-d358-4d99-b900-85902fda9bb0, 
      '2017-02-14 12:43:20-0800', 
      'Raining too hard should have postponed', 
      'Alex',
      [0.45, 0.09, 0.01, 0.2, 0.11]
);
INSERT INTO cycling.comments_vs (record_id, id, created_at, comment, commenter, comment_vector) 
   VALUES (
      now(), 
      e7ae5cf3-d358-4d99-b900-85902fda9bb0, 
      '2017-03-21 13:11:09.999-0800', 
      'Second rest stop was out of water', 
      'Alex',
      [0.99, 0.5, 0.99, 0.1, 0.34]
);
INSERT INTO cycling.comments_vs (record_id, id, created_at, comment, commenter, comment_vector) 
   VALUES (
      now(), 
      e7ae5cf3-d358-4d99-b900-85902fda9bb0, 
      '2017-04-01 06:33:02.16-0800', 
      'LATE RIDERS SHOULD NOT DELAY THE START', 
      'Alex',
      [0.9, 0.54, 0.12, 0.1, 0.95]
);

INSERT INTO cycling.comments_vs (record_id, id, created_at, comment, commenter, comment_vector) 
   VALUES (
      now(), 
      c7fceba0-c141-4207-9494-a29f9809de6f, 
      totimestamp(now()), 
      'The gift certificate for winning was the best', 
      'Amy',
      [0.13, 0.8, 0.35, 0.17, 0.03]
);

INSERT INTO cycling.comments_vs (record_id, id, created_at, comment, commenter, comment_vector) 
   VALUES (
      now(), 
      c7fceba0-c141-4207-9494-a29f9809de6f, 
      '2017-02-17 12:43:20.234+0400', 
      'Glad you ran the race in the rain', 
      'Amy',
      [0.3, 0.34, 0.2, 0.78, 0.25]
);

INSERT INTO cycling.comments_vs (record_id, id, created_at, comment, commenter, comment_vector) 
   VALUES (
      now(), 
      c7fceba0-c141-4207-9494-a29f9809de6f, 
      '2017-03-22 5:16:59.001+0400', 
      'Great snacks at all reststops', 
      'Amy',
      [0.1, 0.4, 0.1, 0.52, 0.09]
);
INSERT INTO cycling.comments_vs (record_id, id, created_at, comment, commenter, comment_vector) 
   VALUES (
      now(), 
      c7fceba0-c141-4207-9494-a29f9809de6f, 
      '2017-04-01 17:43:08.030+0400', 
      'Last climb was a killer', 
      'Amy',
      [0.3, 0.75, 0.2, 0.2, 0.5]
);
// end::insert-vector-data-cycling[]

// tag::select-vector-data-cycling[]
SELECT * FROM cycling.comments_vs 
    ORDER BY comment_vector ANN OF [0.15, 0.1, 0.1, 0.35, 0.55] 
    LIMIT 3;
// end::select-vector-data-cycling[]

// tag::select-vector-data-similarity-cycling[]
SELECT comment, similarity_cosine(comment_vector, [0.2, 0.15, 0.3, 0.2, 0.05])
    FROM cycling.comments_vs
    ORDER BY comment_vector ANN OF [0.1, 0.15, 0.3, 0.12, 0.05] 
    LIMIT 1;
// end::select-vector-data-similarity-cycling[]