CREATE CUSTOM INDEX commenter_idx 
    ON cycling.comments_vs (commenter) 
    USING 'StorageAttachedIndex';
CREATE CUSTOM INDEX created_at_idx 
    ON cycling.comments_vs (created_at) 
    USING 'StorageAttachedIndex';
CREATE CUSTOM INDEX ann_index 
    ON cycling.comments_vs (comment_vector) 
    USING 'StorageAttachedIndex';

