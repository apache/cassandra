CREATE KEYSPACE IF NOT EXISTS cycling
   WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE cycling;

// tag::table-create-cyclist-accounts-simple[]
CREATE TABLE cycling.cyclist_accounts (
  account_holder TEXT PRIMARY KEY,
  account_balance INT,
  account_type TEXT
);
// end::table-create-cyclist-accounts-simple[]

// tag::index-create-account-type[]
CREATE INDEX account_type_idx ON cycling.cyclist_accounts (account_type) USING 'sai';
// end::index-create-account-type[]

// tag::describe-table-cyclist-accounts[]
DESCRIBE TABLE cycling.cyclist_accounts;
// end::describe-table-cyclist-accounts[]

// tag::data-insert-cyclist-accounts[]
INSERT INTO cycling.cyclist_accounts (account_holder, account_balance, account_type) VALUES ('Alice MAYS', 100, 'checking');
INSERT INTO cycling.cyclist_accounts (account_holder, account_balance, account_type) VALUES ('Bob PICKLE', 0, 'checking');
// end::data-insert-cyclist-accounts[]

// tag::select-all-from-cyclist-accounts[]
SELECT * FROM cycling.cyclist_accounts;
// end::select-all-from-cyclist-accounts[]

// tag::transaction-simple[]
BEGIN TRANSACTION
  LET fromBalance = (SELECT account_balance FROM cycling.cyclist_accounts WHERE account_holder = 'Alice MAYS');
  IF fromBalance.account_balance >= 20 THEN
    UPDATE cycling.cyclist_accounts SET account_balance -= 20 WHERE account_holder = 'Alice MAYS';
    UPDATE cycling.cyclist_accounts SET account_balance += 20 WHERE account_holder = 'Bob PICKLE';
  END IF
COMMIT TRANSACTION;
// end::transaction-simple[]

// Test with table-simple
// tag::transaction-using-index-fail[]
BEGIN TRANSACTION
  LET fromBalance = (SELECT account_balance FROM cycling.cyclist_accounts WHERE account_holder = 'Alice MAYS' AND account_type = 'checking');
  IF fromBalance.account_balance >= 20 THEN
    UPDATE cycling.cyclist_accounts SET account_balance -= 20 WHERE account_holder = 'Alice MAYS' AND account_type = 'checking';
    UPDATE cycling.cyclist_accounts SET account_balance += 20 WHERE account_holder = 'Bob PICKLE' AND account_type = 'checking';
  END IF
COMMIT TRANSACTION;
// end::transaction-using-index-fail[]

// tag::transaction-using-index-fail-allow-filtering[]
BEGIN TRANSACTION
  LET fromBalance = (SELECT account_balance FROM cycling.cyclist_accounts WHERE account_holder = 'Alice MAYS' AND account_type = 'checking' ALLOW FILTERING);
  IF fromBalance.account_balance >= 20 THEN
    UPDATE cycling.cyclist_accounts SET account_balance -= 20 WHERE account_holder = 'Alice MAYS' AND account_type = 'checking' ALLOW FILTERING;
    UPDATE cycling.cyclist_accounts SET account_balance += 20 WHERE account_holder = 'Bob PICKLE' AND account_type = 'checking' ALLOW FILTERING;
  END IF
COMMIT TRANSACTION;
// end::transaction-using-index-fail-allow-filtering[]

// tag::table-drop-cyclist-accounts[]
DROP TABLE cycling.cyclist_accounts;
// end::table-drop-cyclist-accounts[]

// tag::table-create-cyclist-accounts-compound[]
CREATE TABLE cycling.cyclist_accounts (
  account_holder TEXT,
  account_balance INT,
  account_type TEXT,
  PRIMARY KEY (account_holder, account_type)
);
// end::table-create-cyclist-accounts-compound[]

// tag::data-insert-cyclist-accounts[]
INSERT INTO cycling.cyclist_accounts (account_holder, account_balance, account_type) VALUES ('Alice MAYS', 100, 'checking');
INSERT INTO cycling.cyclist_accounts (account_holder, account_balance, account_type) VALUES ('Bob PICKLE', 0, 'checking');
// end::data-insert-cyclist-accounts[]

// tag::select-all-from-cyclist-accounts[]
SELECT * FROM cycling.cyclist_accounts;
// end::select-all-from-cyclist-accounts[]

// Test with table-compound
// tag::transaction-using-compound-works[]
BEGIN TRANSACTION
  LET fromBalance = (SELECT account_balance FROM cycling.cyclist_accounts WHERE account_holder = 'Alice MAYS' AND account_type = 'checking');
  IF fromBalance.account_balance >= 20 THEN
    UPDATE cycling.cyclist_accounts SET account_balance -= 20 WHERE account_holder = 'Alice MAYS' AND account_type = 'checking';
    UPDATE cycling.cyclist_accounts SET account_balance += 20 WHERE account_holder = 'Bob PICKLE' AND account_type = 'checking';
  END IF
COMMIT TRANSACTION;
// end::transaction-using-compound-works[]

SELECT * FROM cycling.cyclist_accounts;