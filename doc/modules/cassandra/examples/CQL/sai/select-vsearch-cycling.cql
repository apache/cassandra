SELECT * FROM cycling.comments_vs 
    ORDER BY comment_vector ANN OF [0.15, 0.1, 0.1, 0.35, 0.55] 
    LIMIT 3;
