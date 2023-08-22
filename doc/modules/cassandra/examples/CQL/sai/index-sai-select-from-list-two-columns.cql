SELECT * FROM cycling.upcoming_calendar
    WHERE events CONTAINS 'Criterium du Dauphine' 
          OR month = 7;