UPDATE users SET favs['author'] = 'Ed Poe' WHERE id = 'jsmith';
UPDATE users SET favs = favs + { 'movie' : 'Cassablanca', 'band' : 'ZZ Top' } WHERE id = 'jsmith';
