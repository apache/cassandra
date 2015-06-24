/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.validation.miscellaneous;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

/**
 * Test that TombstoneOverwhelmingException gets thrown when it should be and doesn't when it shouldn't be.
 */
public class TombstonesTest extends CQLTester
{
    static final int ORIGINAL_THRESHOLD = DatabaseDescriptor.getTombstoneFailureThreshold();
    static final int THRESHOLD = 100;

    @BeforeClass
    public static void setUp() throws Throwable
    {
        DatabaseDescriptor.setTombstoneFailureThreshold(THRESHOLD);
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setTombstoneFailureThreshold(ORIGINAL_THRESHOLD);
    }

    @Test
    public void testBelowThresholdSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");

        // insert exactly the amount of tombstones that shouldn't trigger an exception
        for (int i = 0; i < THRESHOLD; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
        }
        catch (Throwable e)
        {
            fail("SELECT with tombstones below the threshold should not have failed, but has: " + e);
        }
    }

    @Test
    public void testBeyondThresholdSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");

        // insert exactly the amount of tombstones that *SHOULD* trigger an exception
        for (int i = 0; i < THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            fail("SELECT with tombstones beyond the threshold should have failed, but hasn't");
        }
        catch (Throwable e)
        {
            String error = "Expected exception instanceof TombstoneOverwhelmingException instead got "
                          + System.lineSeparator()
                          + Throwables.getStackTraceAsString(e);
            assertTrue(error, e instanceof TombstoneOverwhelmingException);
        }
    }

    @Test
    public void testAllShadowedSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");

        // insert exactly the amount of tombstones that *SHOULD* normally trigger an exception
        for (int i = 0; i < THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");

        // delete all with a partition level tombstone
        execute("DELETE FROM %s WHERE a = 'key'");

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
        }
        catch (Throwable e)
        {
            fail("SELECT with tombstones shadowed by a partition tombstone should not have failed, but has: " + e);
        }
    }

    @Test
    public void testLiveShadowedCellsSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b));");

        for (int i = 0; i < THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', 'column');");

        // delete all with a partition level tombstone
        execute("DELETE FROM %s WHERE a = 'key'");

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
        }
        catch (Throwable e)
        {
            fail("SELECT with regular cells shadowed by a partition tombstone should not have failed, but has: " + e);
        }
    }

    @Test
    public void testExpiredTombstones() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b)) WITH gc_grace_seconds = 1;");

        for (int i = 0; i < THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");

        // not yet past gc grace - must throw a TOE
        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            fail("SELECT with tombstones beyond the threshold should have failed, but hasn't");
        }
        catch (Throwable e)
        {
            assertTrue(e instanceof TombstoneOverwhelmingException);
        }

        // sleep past gc grace
        TimeUnit.SECONDS.sleep(2);

        // past gc grace - must not throw a TOE now
        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
        }
        catch (Throwable e)
        {
            fail("SELECT with expired tombstones beyond the threshold should not have failed, but has: " + e);
        }
    }
}
