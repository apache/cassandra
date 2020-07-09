CREATE TYPE custom_type (txt text, i int);
CREATE FUNCTION fct\_using\_udt ( somearg int )
    RETURNS NULL ON NULL INPUT
    RETURNS custom_type
    LANGUAGE java
    AS $$
        UDTValue udt = udfContext.newReturnUDTValue();
        udt.setString("txt", "some string");
        udt.setInt("i", 42);
        return udt;
    $$;
