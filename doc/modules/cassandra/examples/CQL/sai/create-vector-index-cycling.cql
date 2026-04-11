CREATE INDEX IF NOT EXISTS ann_index 
  ON cycling.comments_vs(comment_vector) USING 'sai';