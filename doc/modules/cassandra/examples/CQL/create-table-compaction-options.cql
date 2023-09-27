CREATE TABLE IF NOT EXISTS my_keyspace.my_table
.
.
.
   WITH compaction = {
     'class' : 'LeveledCompactionStrategy',
     'sstable_size_in_mb' : '320' };