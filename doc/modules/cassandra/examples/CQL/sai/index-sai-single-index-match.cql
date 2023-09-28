CREATE CUSTOM INDEX commenter_idx 
    ON cycling.comments_vs (commenter) 
    USING 'StorageAttachedIndex';