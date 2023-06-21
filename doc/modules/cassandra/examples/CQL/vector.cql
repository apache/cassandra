CREATE TABLE plays (
    id text PRIMARY KEY,
    game text,
    players int,
    scores vector<int, 3> // A vector of 3 integers
)

INSERT INTO plays (id, game, players, scores)
           VALUES ('123-afde', 'quake', 3, [17, 4, 2]);

// Replace the existing vector entirely
UPDATE plays SET scores = [ 3, 9, 4] WHERE id = '123-afde';
