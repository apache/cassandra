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

package org.apache.cassandra.db.view;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.junit.Assert.assertEquals;

public class ViewBuilderTaskTest extends CQLTester
{
    private static final ProtocolVersion protocolVersion = ProtocolVersion.CURRENT;

    @Test
    public void testBuildRange() throws Throwable
    {
        requireNetwork();
        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        String tableName = createTable("CREATE TABLE %s (" +
                                       "k int, " +
                                       "c int, " +
                                       "v text, " +
                                       "PRIMARY KEY(k, c))");

        String viewName = tableName + "_view";
        executeNet(protocolVersion, String.format("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                                                  "WHERE v IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL " +
                                                  "PRIMARY KEY (v, k, c)", viewName));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        View view = cfs.keyspace.viewManager.forTable(cfs.metadata().id).iterator().next();

        // Insert the dataset
        for (int k = 0; k < 100; k++)
            for (int c = 0; c < 10; c++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, String.valueOf(k));

        // Retrieve the sorted tokens of the inserted rows
        IPartitioner partitioner = cfs.metadata().partitioner;
        List<Token> tokens = IntStream.range(0, 100)
                                      .mapToObj(Int32Type.instance::decompose)
                                      .map(partitioner::getToken)
                                      .sorted()
                                      .collect(Collectors.toList());

        class Tester
        {
            private void test(int indexOfStartToken,
                              int indexOfEndToken,
                              Integer indexOfLastToken,
                              long keysBuilt,
                              long expectedKeysBuilt,
                              int expectedRowsInView) throws Throwable
            {
                // Truncate the materialized view (not the base table)
                Util.flush(cfs.viewManager);
                cfs.viewManager.truncateBlocking(cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS), System.currentTimeMillis());
                assertRowCount(execute("SELECT * FROM " + viewName), 0);

                // Get the tokens from the referenced inserted rows
                Token startToken = tokens.get(indexOfStartToken);
                Token endToken = tokens.get(indexOfEndToken);
                Token lastToken = indexOfLastToken == null ? null : tokens.get(indexOfLastToken);
                Range<Token> range = new Range<>(startToken, endToken);

                // Run the view build task, verifying the returned number of bult keys
                long actualKeysBuilt = new ViewBuilderTask(cfs, view, range, lastToken, keysBuilt).call();
                assertEquals(expectedKeysBuilt, actualKeysBuilt);

                // Verify that the rows have been written to the MV
                assertRowCount(execute("SELECT * FROM " + viewName), expectedRowsInView);

                // Verify that the last position and number of bult keys have been stored
                assertRows(execute(String.format("SELECT last_token, keys_built " +
                                                 "FROM %s.%s WHERE keyspace_name='%s' AND view_name='%s' " +
                                                 "AND start_token=? AND end_token=?",
                                                 SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                                 SystemKeyspace.VIEW_BUILDS_IN_PROGRESS,
                                                 keyspace(),
                                                 viewName),
                                   startToken.toString(), endToken.toString()),
                           row(endToken.toString(), expectedKeysBuilt));
            }
        }
        Tester tester = new Tester();

        // Build range from rows 0 to 100 without any recorded start position
        tester.test(0, 10, null, 0, 10, 100);

        // Build range from rows 100 to 200 starting at row 150
        tester.test(10, 20, 15, 0, 5, 50);

        // Build range from rows 300 to 400 starting at row 350 with 10 built keys
        tester.test(30, 40, 35, 10, 15, 50);

        // Build range from rows 400 to 500 starting at row 100 (out of range) with 10 built keys
        tester.test(40, 50, 10, 10, 20, 100);

        // Build range from rows 900 to 100 (wrap around) without any recorded start position
        tester.test(90, 10, null, 0, 20, 200);

        executeNet(protocolVersion, "DROP MATERIALIZED VIEW " + view.name);
    }
}
