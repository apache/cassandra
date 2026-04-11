CREATE INDEX commenter_cs_idx ON cycling.comments_vs (commenter)
USING 'sai' 
WITH OPTIONS = {'case_sensitive': 'true', 'normalize': 'true', 'ascii': 'true'};