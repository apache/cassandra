CREATE TABLE durations (
  event text,
  duration tuple<int, text>,
);

INSERT INTO durations (event, duration) VALUES ('ev1', (3, 'hours'));
