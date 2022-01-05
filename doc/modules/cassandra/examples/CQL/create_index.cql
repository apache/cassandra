CREATE INDEX userIndex ON NerdMovies (user);
CREATE INDEX ON Mutants (abilityId);
CREATE INDEX ON users (keys(favs));
CREATE CUSTOM INDEX ON users (email) 
   USING 'path.to.the.IndexClass';
CREATE CUSTOM INDEX ON users (email) 
   USING 'path.to.the.IndexClass' 
   WITH OPTIONS = {'storage': '/mnt/ssd/indexes/'};
