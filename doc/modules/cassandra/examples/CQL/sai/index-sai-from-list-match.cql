CREATE CUSTOM INDEX events_idx 
    ON cycling.upcoming_calendar (events) 
    USING 'StorageAttachedIndex';