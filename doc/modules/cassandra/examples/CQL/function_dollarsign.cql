CREATE FUNCTION some_function ( arg int )
    RETURNS NULL ON NULL INPUT
    RETURNS int
    LANGUAGE java
    AS $$ return arg; $$;

SELECT some_function(column) FROM atable ...;
UPDATE atable SET col = some_function(?) ...;

CREATE TYPE custom_type (txt text, i int);
CREATE FUNCTION fct_using_udt ( udtarg frozen )
    RETURNS NULL ON NULL INPUT
    RETURNS text
    LANGUAGE java
    AS $$ return udtarg.getString("txt"); $$;
