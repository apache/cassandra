CREATE INDEX events_idx 
    ON cycling.upcoming_calendar (events) 
    USING 'sai';