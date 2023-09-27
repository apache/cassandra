CREATE CUSTOM INDEX teams_idx 
    ON cycling.cyclist_career_teams (teams) 
    USING 'StorageAttachedIndex';