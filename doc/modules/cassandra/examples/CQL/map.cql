CREATE TABLE users (
   id text PRIMARY KEY,
   name text,
   favs map<text, text> // A map of text keys, and text values
);

INSERT INTO users (id, name, favs)
   VALUES ('jsmith', 'John Smith', { 'fruit' : 'Apple', 'band' : 'Beatles' });

// Replace the existing map entirely.
UPDATE users SET favs = { 'fruit' : 'Banana' } WHERE id = 'jsmith';
