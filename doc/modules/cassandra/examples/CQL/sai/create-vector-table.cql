CREATE TABLE IF NOT EXISTS cycling.products (
  id int PRIMARY KEY,
  name TEXT,
  description TEXT,
  item_vector VECTOR<FLOAT, 5> //<1>
);
