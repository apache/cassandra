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

package org.apache.cassandra.cql3;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PagingQueryTest extends CQLTester
{
    @Test
    public void pagingOnRegularColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    " k1 int," +
                    " c1 int," +
                    " c2 int," +
                    " v1 text," +
                    " v2 text," +
                    " v3 text," +
                    " v4 text," +
                    "PRIMARY KEY (k1, c1, c2))");

        for (int c1 = 0; c1 < 100; c1++)
        {
            for (int c2 = 0; c2 < 100; c2++)
            {
                execute("INSERT INTO %s (k1, c1, c2, v1, v2, v3, v4) VALUES (?, ?, ?, ?, ?, ?, ?)", 1, c1, c2,
                        Integer.toString(c1), Integer.toString(c2), someText(), someText());
            }

            if (c1 % 30 == 0)
                flush();
        }

        flush();

        try (Session session = sessionNet())
        {
            SimpleStatement stmt = new SimpleStatement("SELECT c1, c2, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE k1 = 1");
            stmt.setFetchSize(3);
            ResultSet rs = session.execute(stmt);
            Iterator<Row> iter = rs.iterator();
            for (int c1 = 0; c1 < 100; c1++)
            {
                for (int c2 = 0; c2 < 100; c2++)
                {
                    assertTrue(iter.hasNext());
                    Row row = iter.next();
                    String msg = "On " + c1 + ',' + c2;
                    assertEquals(msg, c1, row.getInt(0));
                    assertEquals(msg, c2, row.getInt(1));
                    assertEquals(msg, Integer.toString(c1), row.getString(2));
                    assertEquals(msg, Integer.toString(c2), row.getString(3));
                }
            }
            assertFalse(iter.hasNext());

            for (int c1 = 0; c1 < 100; c1++)
            {
                stmt = new SimpleStatement("SELECT c1, c2, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE k1 = 1 AND c1 = ?", c1);
                stmt.setFetchSize(3);
                rs = session.execute(stmt);
                iter = rs.iterator();
                for (int c2 = 0; c2 < 100; c2++)
                {
                    assertTrue(iter.hasNext());
                    Row row = iter.next();
                    String msg = "Within " + c1 + " on " + c2;
                    assertEquals(msg, c1, row.getInt(0));
                    assertEquals(msg, c2, row.getInt(1));
                    assertEquals(msg, Integer.toString(c1), row.getString(2));
                    assertEquals(msg, Integer.toString(c2), row.getString(3));
                }
                assertFalse(iter.hasNext());
            }
        }
    }

    private static String someText()
    {
        char[] arr = new char[1024];
        for (int i = 0; i < arr.length; i++)
            arr[i] = (char)(32 + ThreadLocalRandom.current().nextInt(95));
        return new String(arr);
    }
}
