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
package org.apache.cassandra.schema;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;

@SuppressWarnings("deprecation")
public class LegacySchemaMigratorThriftTest extends LegacySchemaMigratorBaseTest
{
    public static final String KEYSPACE_18956 = "ks18956";
    public static final String TABLE_18956 = "table18956";

    @Test
    public void testMigrate18956() throws IOException
    {
        CQLTester.cleanupAndLeaveDirs();
        Keyspaces expected = LegacySchemaMigratorBaseTest.keyspacesToMigrate18956();
        expected.forEach(LegacySchemaMigratorBaseTest::legacySerializeKeyspace);
        LegacySchemaMigrator.migrate();
        Schema.instance.loadFromDisk();
        LegacySchemaMigratorBaseTest.loadLegacySchemaTables();
        try {
            // This should fail
            executeOnceInternal(String.format("ALTER TABLE %s.%s RENAME key TO \"4f\"", KEYSPACE_18956, TABLE_18956));
            assert false;
        } catch (InvalidRequestException e) {
            assert e.toString().contains("another column of that name already exist");
        }
    }

}
