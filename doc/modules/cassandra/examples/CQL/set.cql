CREATE TABLE images (
   name text PRIMARY KEY,
   owner text,
   tags set<text> // A set of text values
);

INSERT INTO images (name, owner, tags)
   VALUES ('cat.jpg', 'jsmith', { 'pet', 'cute' });

// Replace the existing set entirely
UPDATE images SET tags = { 'kitten', 'cat', 'lol' } WHERE name = 'cat.jpg';
