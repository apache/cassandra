SOURCE 'race_times-table.cql';

// tag::duration[]
INSERT INTO cycling.race_times (
  finish_time,
  race_name, cyclist_name, race_date
) VALUES (
  1h4m48s20ms,
  '17th Santos Tour Down Under', 'Rohan DENNIS', '2017-04-14'
);
// end::duration[]

// tag::update_duration[]
UPDATE cycling.race_times
SET finish_time = 2h5m22s14ms
WHERE race_name = '17th Santos Tour Down Under'
  AND cyclist_name = 'Rohan DENNIS'
  AND race_date = '2017-04-14';
// end::update_duration[]

CAPTURE 'select_all_from_race_times.results';
// tag::select_duration[]
SELECT *
FROM cycling.race_times
WHERE race_name = '17th Santos Tour Down Under'
  AND cyclist_name = 'Rohan DENNIS'
  AND race_date = '2017-04-14';
// end::select_duration[]
CAPTURE OFF;

// tag::select_race_times_where_race_date_is_calculation[]
SELECT *
FROM cycling.race_times
WHERE race_date = '2017-04-15' - 1d;
// end::select_race_times_where_race_date_is_calculation[]

// tag::rename[]
ALTER TABLE cycling.race_times
RENAME race_date TO date;
// end::rename[]

ALTER TABLE cycling.race_times
RENAME date TO race_date;