SELECT * FROM posts
 WHERE token(userid) > token('tom') AND token(userid) < token('bob');
