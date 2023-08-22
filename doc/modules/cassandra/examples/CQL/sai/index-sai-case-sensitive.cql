CREATE CUSTOM INDEX commenter_cs_idx ON cycling.comments_vs (commenter)
USING 'StorageAttachedIndex' 
WITH OPTIONS = {'case_sensitive': 'true', 'normalize': 'true', 'ascii': 'true'};