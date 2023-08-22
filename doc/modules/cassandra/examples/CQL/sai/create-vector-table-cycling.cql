// tag::createvstable[]
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
// end::createvstable[]

// tag::altervstable[]
ALTER TABLE cycling.comments_vs
    ADD comment_vector VECTOR <FLOAT, 5>; //<1>
// end::altervstable[]