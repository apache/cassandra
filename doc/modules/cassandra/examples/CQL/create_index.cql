CREATE INDEX userIndex ON NerdMovies (user);
CREATE INDEX ON Mutants (abilityId);
CREATE INDEX ON users (KEYS(favs));
CREATE INDEX ON users (age) USING 'sai';
CREATE CUSTOM INDEX ON users (email) 
   USING 'path.to.the.IndexClass';
CREATE CUSTOM INDEX ON users (email) 
   USING 'path.to.the.IndexClass' 
   WITH OPTIONS = {'storage': '/mnt/ssd/indexes/'};
