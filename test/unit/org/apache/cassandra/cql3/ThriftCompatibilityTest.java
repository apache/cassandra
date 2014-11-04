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

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;

import static org.junit.Assert.assertEquals;

public class ThriftCompatibilityTest extends SchemaLoader
{
    private static UntypedResultSet execute(String query) throws Throwable
    {
        try
        {
            return QueryProcessor.executeInternal(String.format(query));
        }
        catch (RuntimeException exc)
        {
            if (exc.getCause() != null)
                throw exc.getCause();
            throw exc;
        }
    }

    /** Test For CASSANDRA-8178 */
    @Test
    public void testNonTextComparator() throws Throwable
    {
        // the comparator is IntegerType, and there is a column named 42 with a UTF8Type validation type
        execute("INSERT INTO \"Keyspace1\".\"JdbcInteger\" (key, \"42\") VALUES (0x00000001, 'abc')");
        execute("UPDATE \"Keyspace1\".\"JdbcInteger\" SET \"42\" = 'abc' WHERE key = 0x00000001");
        execute("DELETE \"42\" FROM \"Keyspace1\".\"JdbcInteger\" WHERE key = 0x00000000");
        UntypedResultSet results = execute("SELECT key, \"42\" FROM \"Keyspace1\".\"JdbcInteger\"");
        assertEquals(1, results.size());
        UntypedResultSet.Row row = results.iterator().next();
        assertEquals(ByteBufferUtil.bytes(1), row.getBytes("key"));
        assertEquals("abc", row.getString("42"));
    }
}
