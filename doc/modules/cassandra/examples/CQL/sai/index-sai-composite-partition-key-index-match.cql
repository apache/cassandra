CREATE INDEX race_name_idx 
    ON cycling.rank_by_year_and_name (race_name) 
    USING 'sai';
CREATE INDEX race_year_idx 
    ON cycling.rank_by_year_and_name (race_year) 
    USING 'sai';