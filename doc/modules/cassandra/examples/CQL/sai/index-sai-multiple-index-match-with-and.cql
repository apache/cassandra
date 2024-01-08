CREATE INDEX commenter_idx 
    ON cycling.comments_vs (commenter) 
    USING 'sai';
CREATE INDEX created_at_idx 
    ON cycling.comments_vs (created_at) 
    USING 'sai';
CREATE INDEX ann_index 
    ON cycling.comments_vs (comment_vector) 
    USING 'sai';

