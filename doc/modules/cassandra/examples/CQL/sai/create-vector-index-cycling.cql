CREATE CUSTOM INDEX IF NOT EXISTS ann_index 
  ON cycling.comments_vs(comment_vector) USING 'StorageAttachedIndex';