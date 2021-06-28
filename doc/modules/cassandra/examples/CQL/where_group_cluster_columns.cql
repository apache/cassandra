SELECT * FROM posts
 WHERE userid = 'john doe'
   AND (blog_title, posted_at) > ('John''s Blog', '2012-01-01');
