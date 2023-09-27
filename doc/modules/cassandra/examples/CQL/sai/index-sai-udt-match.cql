CREATE CUSTOM INDEX races_idx 
    ON cycling.cyclist_races (races) 
    USING 'StorageAttachedIndex';