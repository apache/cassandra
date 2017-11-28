package org.apache.cassandra.cql3.validation.operations;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.junit.Test;

public class TTLTest extends CQLTester
{

    @Test
    public void testTTLPerRequestLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        // insert
        execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", Attributes.MAX_TTL); // max ttl
        int ttl = execute("SELECT ttl(i) FROM %s").one().getInt("ttl(i)");
        assertTrue(ttl > Attributes.MAX_TTL - 10);

        try
        {
            execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", Attributes.MAX_TTL + 1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("ttl is too large."));
        }

        try
        {
            execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", -1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("A TTL must be greater or equal to 0, but was -1"));
        }
        execute("TRUNCATE %s");

        // update
        execute("UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", Attributes.MAX_TTL); // max ttl
        ttl = execute("SELECT ttl(i) FROM %s").one().getInt("ttl(i)");
        assertTrue(ttl > Attributes.MAX_TTL - 10);

        try
        {
            execute("UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", Attributes.MAX_TTL + 1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("ttl is too large."));
        }

        try
        {
            execute("UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", -1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("A TTL must be greater or equal to 0, but was -1"));
        }
    }

    @Test
    public void testTTLDefaultLimit() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live=-1");
            fail("Expect Invalid schema");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getCause()
                        .getMessage()
                        .contains("default_time_to_live must be greater than or equal to 0 (got -1)"));
        }
        try
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live="
                    + (Attributes.MAX_TTL + 1));
            fail("Expect Invalid schema");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getCause()
                        .getMessage()
                        .contains("default_time_to_live must be less than or equal to " + Attributes.MAX_TTL + " (got "
                                + (Attributes.MAX_TTL + 1) + ")"));
        }

        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live=" + Attributes.MAX_TTL);
        execute("INSERT INTO %s (k, i) VALUES (1, 1)");
        int ttl = execute("SELECT ttl(i) FROM %s").one().getInt("ttl(i)");
        assertTrue(ttl > 10000 - 10); // within 10 second
    }

}
