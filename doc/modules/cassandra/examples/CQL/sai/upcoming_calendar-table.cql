SOURCE '0_create_keyspace.cql';

DROP TABLE IF EXISTS cycling.upcoming_calendar;

// Find all calendar events for a particular year and month
// tag::listColumn[]
CREATE TABLE IF NOT EXISTS cycling.upcoming_calendar (
  year int,
  month int,
  events list<text>,
  PRIMARY KEY (year, month)
);
// end::listColumn[]

// tag::add_tour[]
INSERT INTO cycling.upcoming_calendar (
  year, month, events
) VALUES (
  2015, 06, [ 'Criterium du Dauphine', 'Tour de Suisse' ]
);
// end::add_tour[]

INSERT INTO cycling.upcoming_calendar (
  year, month, events
) VALUES (
  2015, 07, [ 'Tour de France' ]
);

// tag::delete[]
DELETE events[0]
FROM cycling.upcoming_calendar
WHERE year = 2015
  AND month = 07;
// end::delete[]

// tag::remove[]
UPDATE cycling.upcoming_calendar
SET events = events - ['Tour de France Stage 10']
WHERE year = 2015
  AND month = 07;
// end::remove[]

INSERT INTO cycling.upcoming_calendar (
  year, month, events
) VALUES (
  2015, 07, [ 'Tour de France' ]
);

CAPTURE 'select_all_from_upcoming_calendar.result';
// tag::select[]
SELECT *
FROM cycling.upcoming_calendar;
// end::select[]
CAPTURE OFF;

// tag::insert[]
UPDATE cycling.upcoming_calendar 
SET events = [ 'Criterium du Dauphine', 'Tour de Suisse' ]
WHERE year = 2015
  AND month = 06;
// end::insert[]

// tag::prepend[]
UPDATE cycling.upcoming_calendar 
SET events = [ 'Tour de France' ] + events 
WHERE year = 2015
  AND month = 06;
// end::prepend[]

// tag::append[]
UPDATE cycling.upcoming_calendar 
SET events = events + [ 'Tour de France' ] 
WHERE year = 2017
  AND month = 05;
// end::append[]

// tag::position[]
UPDATE cycling.upcoming_calendar 
SET events[2] = 'Tour de France' 
WHERE year = 2015
  AND month = 06;
// end::position[]

// tag::set[]
UPDATE cycling.upcoming_calendar 
SET events[2] = 'Vuelta Ciclista a Venezuela' 
WHERE year = 2015
  AND month = 06;
// end::set[]

// tag::ttl[]
UPDATE cycling.upcoming_calendar 
USING TTL 10000000
SET events[2] = 'Vuelta Ciclista a Venezuela' 
WHERE year = 2015
  AND month = 06;
// end::ttl[]

// tag::addcolumn[]
ALTER TABLE cycling.upcoming_calendar 
ADD description map<text,text>;
// end::addcolumn[]

// tag::example[]
UPDATE cycling.upcoming_calendar 
SET description = description + {
  'Criterium du Dauphine' : 'Easy race', 'Tour du Suisse' : 'Hard uphill race'
}
WHERE year = 2015
  AND month = 6;
// end::example[]

// tag::newrow[]
UPDATE cycling.upcoming_calendar 
SET description = {
  'Criterium du Dauphine' : 'Easy race', 'Tour du Suisse' : 'Hard uphill race'
} 
WHERE year = 2015
  AND month = 6;
// end::newrow[]

CAPTURE 'select_description_from_upcoming_calendar.result';
// tag::select_description[]
SELECT description
FROM cycling.upcoming_calendar
WHERE year = 2015
  AND month = 6;
// end::select_description[]
CAPTURE OFF;

// tag::value[]
UPDATE cycling.upcoming_calendar 
SET events = events - [ 'Tour de France' ] 
WHERE year = 2015
  AND month = 06;
// end::value[]

// tag::map[]
UPDATE cycling.upcoming_calendar 
SET description = description + {
  'Criterium du Dauphine' : 'Easy race'
} 
WHERE year = 2015
  AND month = 06;
// end::map[]
