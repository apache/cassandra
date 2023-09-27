CREATE CUSTOM INDEX sim_comments_idx 
    ON cycling.comments_vs (comment_vector) 
    USING 'StorageAttachedIndex'  
    WITH OPTIONS = { 'similarity_function': 'DOT_PRODUCT'};