SOURCE 'keyspace-create-simple.cql';
SOURCE 'keyspace-use.cql';

DROP TABLE IF EXISTS cycling.cyclist_expenses;

// tag::create_table[]
CREATE TABLE IF NOT EXISTS cycling.cyclist_expenses (
  cyclist_name text,
  balance float STATIC,
  expense_id int,
  amount float,
  description text,
  paid boolean,
  PRIMARY KEY (cyclist_name, expense_id)
);
// end::create_table[]

// tag::batch_Vera[]
BEGIN BATCH

  INSERT INTO cycling.cyclist_expenses (
    cyclist_name, balance
  ) VALUES (
    'Vera ADRIAN', 0
  ) IF NOT EXISTS;

  INSERT INTO cycling.cyclist_expenses (
    cyclist_name, expense_id, amount, description, paid
  ) VALUES (
    'Vera ADRIAN', 1, 7.95, 'Breakfast', false
  );

APPLY BATCH;
// end::batch_Vera[]

CAPTURE 'select_initial_from_cyclist_expenses.results';
// tag::select_initial[]
SELECT *
FROM cycling.cyclist_expenses;
// end::select_initial[]
CAPTURE OFF;

// tag::update_Vera[]
UPDATE cycling.cyclist_expenses
SET balance = -7.95
WHERE cyclist_name = 'Vera ADRIAN'
IF balance = 0;
// end::update_Vera[]

CAPTURE 'select_after_update_from_cyclist_expenses.results';
SELECT *
FROM cycling.cyclist_expenses;
CAPTURE OFF;

// tag::batch_food[]
BEGIN BATCH

  INSERT INTO cycling.cyclist_expenses (
    cyclist_name, expense_id, amount, description, paid
  ) VALUES (
    'Vera ADRIAN', 2, 13.44, 'Lunch', true
  );

  INSERT INTO cycling.cyclist_expenses (
    cyclist_name, expense_id, amount, description, paid
  ) VALUES (
    'Vera ADRIAN', 3, 25.00, 'Dinner', false
  );

  UPDATE cycling.cyclist_expenses
  SET balance = -32.95
  WHERE cyclist_name = 'Vera ADRIAN'
  IF balance = -7.95;

APPLY BATCH;
// end::batch_food[]

CAPTURE 'select_after_batch_food_from_cyclist_expenses.results';
SELECT *
FROM cycling.cyclist_expenses;
CAPTURE OFF;

// tag::batch_paid[]
BEGIN BATCH

  UPDATE cycling.cyclist_expenses
  SET balance = 0
  WHERE cyclist_name = 'Vera ADRIAN'
  IF balance = -32.95;

  UPDATE cycling.cyclist_expenses
  SET paid = true
  WHERE cyclist_name = 'Vera ADRIAN'
  AND expense_id = 1 IF paid = false;

  UPDATE cycling.cyclist_expenses
  SET paid = true
  WHERE cyclist_name = 'Vera ADRIAN'
  AND expense_id = 3
  IF paid = false;

APPLY BATCH;
// end::batch_paid[]

CAPTURE 'select_after_batch_paid_from_cyclist_expenses.results';
SELECT *
FROM cycling.cyclist_expenses;
CAPTURE OFF;

// tag::batch_multiple_partitions[]
BEGIN BATCH

  INSERT INTO cycling.cyclist_expenses (
    cyclist_name, expense_id, amount, description, paid
  ) VALUES (
    'John SMITH', 3, 15.00, 'Lunch', false
  );

  INSERT INTO cycling.cyclist_name (
    id, lastname, firstname
  ) VALUES (
    6ab09bec-e68e-48d9-a5f8-97e6fb4c9b12, 'SMITH', 'John'
  );

APPLY BATCH;
// end::batch_multiple_partitions[]

DELETE FROM cycling.cyclist_name
WHERE id = 6ab09bec-e68e-48d9-a5f8-97e6fb4c9b12;

TRUNCATE TABLE cycling.cyclist_expenses;

// tag::batch[]
BEGIN BATCH USING TIMESTAMP 1481124356754405

  INSERT INTO cycling.cyclist_expenses (
    cyclist_name, expense_id, amount, description, paid
  ) VALUES (
    'Vera ADRIAN', 2, 13.44, 'Lunch', true
  );

  INSERT INTO cycling.cyclist_expenses (
    cyclist_name, expense_id, amount, description, paid
  ) VALUES (
    'Vera ADRIAN', 3, 25.00, 'Dinner', true
  );

APPLY BATCH;
// end::batch[]

// tag::con[]
BEGIN BATCH

  INSERT INTO cycling.cyclist_expenses (
    cyclist_name, expense_id
  ) VALUES (
    'Joe WALLS', 1
  )
  IF NOT EXISTS;

  INSERT INTO cycling.cyclist_expenses (
    cyclist_name, expense_id, amount, description, paid
  ) VALUES (
    'Joe WALLS', 1, 8, 'burrito', false
  );

APPLY BATCH;
// end::con[]

insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',1,50,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',2,50,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Michael MATTHEWS',3,25,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Michael MATTHEWS',4,25,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Alex FRAME',5,40,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Alex FRAME',6,40,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Steven KRUIKSWIJK',7,20,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Steven KRUIKSWIJK',8,39,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Marianne VOS',9,20,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Marianne VOS',10,20,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Paolo TIRALONGO',11,10,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Paolo TIRALONGO',12,10,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',13,50,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',14,25,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',15,35,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',16,20,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',17,70,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',18,10,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',19,50,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',20,25,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',21,35,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',22,20,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Anna VAN DER BREGGEN',23,70,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Paolo TIRALONGO',24,10,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Paolo TIRALONGO',25,11,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Paolo TIRALONGO',26,12,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Paolo TIRALONGO',27,13,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Paolo TIRALONGO',28,14,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Paolo TIRALONGO',29,15,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Paolo TIRALONGO',30,16,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Paolo TIRALONGO',31,17,'dinner');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Paolo TIRALONGO',32,18,'breakfast');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Steven KRUIKSWIJK',33,3,'coffee');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Steven KRUIKSWIJK',34,3,'coffee');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Steven KRUIKSWIJK',35,3,'coffee');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Steven KRUIKSWIJK',36,3,'coffee');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Steven KRUIKSWIJK',37,3,'coffee');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Steven KRUIKSWIJK',38,3,'coffee');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Steven KRUIKSWIJK',39,3,'coffee');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Michael MATTHEWS',40,25,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Michael MATTHEWS',41,25,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Michael MATTHEWS',42,25,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Michael MATTHEWS',43,25,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Michael MATTHEWS',44,25,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Michael MATTHEWS',45,25,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Michael MATTHEWS',46,25,'lunch');
insert into cycling.cyclist_expenses (cyclist_name,expense_id,amount,description) values ('Michael MATTHEWS',47,25,'lunch');
