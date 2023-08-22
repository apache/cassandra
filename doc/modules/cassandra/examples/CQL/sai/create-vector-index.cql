CREATE CUSTOM INDEX IF NOT EXISTS ann_index 
  ON vsearch.products(item_vector) USING 'StorageAttachedIndex';