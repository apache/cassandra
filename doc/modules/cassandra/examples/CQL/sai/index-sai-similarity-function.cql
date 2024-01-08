CREATE INDEX sim_comments_idx 
    ON cycling.comments_vs (comment_vector) 
    USING 'sai'  
    WITH OPTIONS = { 'similarity_function': 'DOT_PRODUCT'};