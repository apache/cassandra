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

package org.apache.cassandra.cql3.validation.operations;

import java.util.Arrays;
import java.util.EnumSet;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SchemaCQLHelper;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.junit.Assert.assertTrue;

public class CompactStorageSplit2Test extends CQLTester
{
    @Test
    public void testFilteringOnCompactTablesWithoutIndices() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, 7)");

        // Adds tomstones
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 2, 7)");
        execute("DELETE FROM %s WHERE a = 1 AND b = 1");
        execute("DELETE FROM %s WHERE a = 2 AND b = 2");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4 ALLOW FILTERING"),
                       row(1, 4, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7)");

            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING"),
                       row(1, 3, 6),
                       row(2, 3, 7));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > 4");

            assertRows(execute("SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                       row(1, 3, 6),
                       row(2, 3, 7));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= 4");

            assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= 3 AND c <= 6");

            assertRows(execute("SELECT * FROM %s WHERE c >= 3 AND c <= 6 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(1, 3, 6),
                       row(1, 4, 4));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, 7)");

        // Adds tomstones
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (5, 2, 7)");
        execute("DELETE FROM %s WHERE a = 0");
        execute("DELETE FROM %s WHERE a = 5");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7)");

            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING"),
                       row(2, 1, 6));


            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > 4");

            assertRows(execute("SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= 4");

            assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= 3 AND c <= 6");

            assertRows(execute("SELECT * FROM %s WHERE c >= 3 AND c <= 6 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(2, 1, 6),
                       row(3, 2, 4));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");

        // // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testFilteringOnCompactTablesWithoutIndicesAndWithLists() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<list<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, [6, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, [4, 1])");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, [7, 1])");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = [4, 1]");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = [4, 1] ALLOW FILTERING"),
                       row(1, 4, list(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE c > [4, 2] ALLOW FILTERING"),
                       row(1, 3, list(6, 2)),
                       row(2, 3, list(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b <= 3 AND c < [6, 2]");

            assertRows(execute("SELECT * FROM %s WHERE b <= 3 AND c < [6, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= [4, 2] AND c <= [6, 4]");

            assertRows(execute("SELECT * FROM %s WHERE c >= [4, 2] AND c <= [6, 4] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(1, 3, list(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(1, 3, list(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, list(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<list<int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, [6, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, [4, 1])");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, [7, 1])");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = [4, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE c > [4, 2] ALLOW FILTERING"),
                       row(2, 1, list(6, 2)),
                       row(4, 1, list(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= [4, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(3, 2, list(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= [4, 3] AND c <= [7]");

            assertRows(execute("SELECT * FROM %s WHERE c >= [4, 3] AND c <= [7] ALLOW FILTERING"),
                       row(2, 1, list(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(2, 1, list(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, list(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testFilteringOnCompactTablesWithoutIndicesAndWithSets() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<set<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, {6, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, {4, 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, {7, 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4, 1}");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4, 1} ALLOW FILTERING"),
                       row(1, 4, set(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE c > {4, 2} ALLOW FILTERING"),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b <= 3 AND c < {6, 2}");

            assertRows(execute("SELECT * FROM %s WHERE b <= 3 AND c < {6, 2} ALLOW FILTERING"),
                       row(1, 2, set(2, 4)),
                       row(2, 3, set(1, 7)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4, 2} AND c <= {6, 4}");

            assertRows(execute("SELECT * FROM %s WHERE c >= {4, 2} AND c <= {6, 4} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, set(6, 2)));
        });
        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<set<int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, {6, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, {4, 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, {7, 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4, 2} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE c > {4, 2} ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= {4, 2} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(4, 1, set(1, 7)),
                       row(3, 2, set(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4, 3} AND c <= {7}");

            assertRows(execute("SELECT * FROM %s WHERE c >= {5, 2} AND c <= {7} ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testAllowFilteringOnPartitionKeyWithDistinct() throws Throwable
    {
        // Test a 'compact storage' table.
        createTable("CREATE TABLE %s (pk0 int, pk1 int, val int, PRIMARY KEY((pk0, pk1))) WITH COMPACT STORAGE");

        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (pk0, pk1, val) VALUES (?, ?, ?)", i, i, i);

        beforeAndAfterFlush(() -> {
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT DISTINCT pk0, pk1 FROM %s WHERE pk1 = 1 LIMIT 3");

            assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s WHERE pk0 < 2 AND pk1 = 1 LIMIT 1 ALLOW FILTERING"),
                       row(1, 1));

            assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s WHERE pk1 > 1 LIMIT 3 ALLOW FILTERING"),
                       row(2, 2));
        });

        // Test a 'wide row' thrift table.
        createTable("CREATE TABLE %s (pk int, name text, val int, PRIMARY KEY(pk, name)) WITH COMPACT STORAGE");

        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (pk, name, val) VALUES (?, 'name0', 0)", i);
            execute("INSERT INTO %s (pk, name, val) VALUES (?, 'name1', 1)", i);
        }

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT DISTINCT pk FROM %s WHERE pk > 1 LIMIT 1 ALLOW FILTERING"),
                       row(2));

            assertRows(execute("SELECT DISTINCT pk FROM %s WHERE pk > 0 LIMIT 3 ALLOW FILTERING"),
                       row(1),
                       row(2));
        });
    }

    @Test
    public void testAllowFilteringOnPartitionKeyWithCounters() throws Throwable
    {
        for (String compactStorageClause : new String[]{ "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (a int, b int, c int, cnt counter, PRIMARY KEY ((a, b), c))"
                        + compactStorageClause);

            execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 14L, 11, 12, 13);
            execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 21, 22, 23);
            execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 27L, 21, 22, 26);
            execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 34L, 31, 32, 33);
            execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 41, 42, 43);

            beforeAndAfterFlush(() -> {

                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt = 24"),
                           row(41, 42, 43, 24L),
                           row(21, 22, 23, 24L));
                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 22 AND cnt = 24"),
                           row(41, 42, 43, 24L));
                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND b < 25 AND cnt = 24"),
                           row(21, 22, 23, 24L));
                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND c < 25 AND cnt = 24"),
                           row(21, 22, 23, 24L));

                assertInvalidMessage(
                "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.",
                "SELECT * FROM %s WHERE a = 21 AND b > 10 AND cnt > 23 ORDER BY c DESC ALLOW FILTERING");

                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a = 21 AND b = 22 AND cnt > 23 ORDER BY c DESC"),
                           row(21, 22, 26, 27L),
                           row(21, 22, 23, 24L));

                assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt > 20 AND cnt < 30"),
                           row(41, 42, 43, 24L),
                           row(21, 22, 23, 24L),
                           row(21, 22, 26, 27L));
            });
        }
    }

    @Test
    public void testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndicesAndWithLists() throws Throwable
    {
        // ----------------------------------------------
        // Test COMPACT table with clustering columns
        // ----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<list<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, [6, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, [4, 1])");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, [7, 1])");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = [4, 1]");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b >= 4 AND c = [4, 1] ALLOW FILTERING"),
                       row(1, 4, list(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 0 AND c > [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c > [4, 2] ALLOW FILTERING"),
                       row(1, 3, list(6, 2)),
                       row(2, 3, list(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND b <= 3 AND c < [6, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a <= 1 AND b <= 3 AND c < [6, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a <= 1 AND c >= [4, 2] AND c <= [6, 4]");

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND b <= 3 AND c >= [4, 2] AND c <= [6, 4] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(1, 3, list(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(1, 3, list(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a > 1 AND c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE a < 2 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, list(6, 2)));
        });
        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());

        // ----------------------------------------------
        // Test COMPACT table without clustering columns
        // ----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<list<int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, [4, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, [6, 2])");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, [4, 1])");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, [7, 1])");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = [4, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c > [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a > 3 AND c > [4, 2] ALLOW FILTERING"),
                       row(4, 1, list(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a < 1 AND b < 3 AND c <= [4, 2]");

            assertRows(execute("SELECT * FROM %s WHERE a < 3 AND b < 3 AND c <= [4, 2] ALLOW FILTERING"),
                       row(1, 2, list(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c >= [4, 3] AND c <= [7]");

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND c >= [4, 3] AND c <= [7] ALLOW FILTERING"),
                       row(2, 1, list(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 3 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, list(4, 2)),
                       row(2, 1, list(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a >=1 AND c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE a < 3 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, list(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a > 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());
    }


    @Test
    public void testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndicesAndWithMaps() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<map<int, int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, {6 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, {4 : 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, {7 : 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = {4 : 1}");

            assertRows(execute("SELECT * FROM %s WHERE a <= 1 AND b = 4 AND c = {4 : 1} ALLOW FILTERING"),
                       row(1, 4, map(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c > {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a > 1 AND c > {4 : 2} ALLOW FILTERING"),
                       row(2, 3, map(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND b <= 3 AND c < {6 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b <= 3 AND c < {6 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 1 AND c >= {4 : 2} AND c <= {6 : 4}");

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND c >= {4 : 2} AND c <= {6 : 4} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(1, 3, map(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a > 10 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(1, 3, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE a < 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, map(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<map<int, int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, {6 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, {4 : 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, {7 : 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c > {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c > {4 : 2} ALLOW FILTERING"),
                       row(2, 1, map(6, 2)),
                       row(4, 1, map(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b < 3 AND c <= {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b < 3 AND c <= {4 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(3, 2, map(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c >= {4 : 3} AND c <= {7 : 1}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND c >= {5 : 2} AND c <= {7 : 0} ALLOW FILTERING"),
                       row(2, 1, map(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(2, 1, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND c CONTAINS KEY 4 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(3, 2, map(4, 1)));

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(2, 1, map(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndicesAndWithSets() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<set<int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, {6, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, {4, 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, {7, 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = {4, 1}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b = 4 AND c = {4, 1} ALLOW FILTERING"),
                       row(1, 4, set(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c > {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c > {4, 2} ALLOW FILTERING"),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b <= 3 AND c < {6, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a > 0 AND b <= 3 AND c < {6, 2} ALLOW FILTERING"),
                       row(1, 2, set(2, 4)),
                       row(2, 3, set(1, 7)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c >= {4, 2} AND c <= {6, 4}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 0 AND c >= {4, 2} AND c <= {6, 4} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a < 2 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(1, 3, set(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(1, 3, set(6, 2)));
        });
        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<set<int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, {6, 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, {4, 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, {7, 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b = 2 AND c = {4, 2} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c > {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND c > {4, 2} ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND b < 3 AND c <= {4, 2}");

            assertRows(execute("SELECT * FROM %s WHERE a <= 4 AND b < 3 AND c <= {4, 2} ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(4, 1, set(1, 7)),
                       row(3, 2, set(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c >= {4, 3} AND c <= {7}");

            assertRows(execute("SELECT * FROM %s WHERE a < 3 AND c >= {5, 2} AND c <= {7} ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE a >= 0 AND c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, set(4, 2)),
                       row(2, 1, set(6, 2)));

            assertInvalidMessage("Cannot use CONTAINS KEY on non-map column c",
                                 "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS KEY 2 ALLOW FILTERING");

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND c CONTAINS 2 AND c CONTAINS 6 ALLOW FILTERING"),
                       row(2, 1, set(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE a >= 1 AND c CONTAINS ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void testAllowFilteringOnPartitionKeyOnCompactTablesWithoutIndices() throws Throwable
    {
        // ----------------------------------------------
        // Test COMPACT table with clustering columns
        // ----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b), c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 2, 4, 5)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 3, 6, 7)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 4, 4, 5)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, 3, 7, 8)");

        // Adds tomstones
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 1, 4, 5)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (2, 2, 7, 8)");
        execute("DELETE FROM %s WHERE a = 1 AND b = 1 AND c = 4");
        execute("DELETE FROM %s WHERE a = 2 AND b = 2 AND c = 7");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4"),
                       row(1, 4, 4, 5));

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4 AND d = 5");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4 ALLOW FILTERING"),
                       row(1, 4, 4, 5));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a IN (1, 2) AND b = 3 AND d IN (6, 7)");

            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2) AND b = 3 AND d IN (6, 7) ALLOW FILTERING"),
                       row(1, 3, 6, 7));

            assertRows(execute("SELECT * FROM %s WHERE a < 2 AND c > 4 AND c <= 6 ALLOW FILTERING"),
                       row(1, 3, 6, 7));

            assertRows(execute("SELECT * FROM %s WHERE a <= 1 AND b >= 2 AND c >= 4 AND d <= 8 ALLOW FILTERING"),
                       row(1, 3, 6, 7),
                       row(1, 4, 4, 5),
                       row(1, 2, 4, 5));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND c >= 4 AND d <= 8 ALLOW FILTERING"),
                       row(1, 3, 6, 7),
                       row(1, 4, 4, 5),
                       row(1, 2, 4, 5));

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND c >= 4 AND d <= 8 ALLOW FILTERING"),
                       row(2, 3, 7, 8));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE d = null");
        assertInvalidMessage("Unsupported null value for column a",
                             "SELECT * FROM %s WHERE a = null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column a",
                             "SELECT * FROM %s WHERE a > null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column a",
                             "SELECT * FROM %s WHERE a = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column a",
                             "SELECT * FROM %s WHERE a > ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int primary key, b int, c int) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, 6)");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, 7)");

        // Adds tomstones
        execute("INSERT INTO %s (a, b, c) VALUES (0, 1, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES (5, 2, 7)");
        execute("DELETE FROM %s WHERE a = 0");
        execute("DELETE FROM %s WHERE a = 5");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = 4");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE b >= 2 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE b >= 2 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a >= 2 AND b <=1 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7));

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND c >= 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b IN (1, 2) AND c IN (6, 7)");

            assertRows(execute("SELECT * FROM %s WHERE a IN (1, 2) AND c IN (6, 7) ALLOW FILTERING"),
                       row(2, 1, 6));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > 4");

            assertRows(execute("SELECT * FROM %s WHERE c > 4 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7));

            assertRows(execute("SELECT * FROM %s WHERE a >= 1 AND b >= 2 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a < 3 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE a < 3 AND b >= 2 AND c <= 4 ALLOW FILTERING"),
                       row(1, 2, 4));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= 3 AND c <= 6");

            assertRows(execute("SELECT * FROM %s WHERE c <=6 ALLOW FILTERING"),
                       row(1, 2, 4),
                       row(2, 1, 6),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE token(a) >= token(2)"),
                       row(2, 1, 6),
                       row(4, 1, 7),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE token(a) >= token(2) ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7),
                       row(3, 2, 4));

            assertRows(execute("SELECT * FROM %s WHERE token(a) >= token(2) AND b = 1 ALLOW FILTERING"),
                       row(2, 1, 6),
                       row(4, 1, 7));
        });
    }

    @Test
    public void testFilteringOnCompactTablesWithoutIndicesAndWithMaps() throws Throwable
    {
        //----------------------------------------------
        // Test COMPACT table with clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int, b int, c frozen<map<int, int>>, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 3, {6 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 4, {4 : 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 3, {7 : 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4 : 1}");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 4 AND c = {4 : 1} ALLOW FILTERING"),
                       row(1, 4, map(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE c > {4 : 2} ALLOW FILTERING"),
                       row(1, 3, map(6, 2)),
                       row(2, 3, map(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b <= 3 AND c < {6 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE b <= 3 AND c < {6 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4 : 2} AND c <= {6 : 4}");

            assertRows(execute("SELECT * FROM %s WHERE c >= {4 : 2} AND c <= {6 : 4} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(1, 3, map(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(1, 3, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(1, 3, map(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY ? ALLOW FILTERING",
                             unset());

        //----------------------------------------------
        // Test COMPACT table without clustering columns
        //----------------------------------------------
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c frozen<map<int, int>>) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, {4 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, {6 : 2})");
        execute("INSERT INTO %s (a, b, c) VALUES (3, 2, {4 : 1})");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 1, {7 : 1})");

        beforeAndAfterFlush(() -> {

            // Checks filtering
            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = {4 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c > {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE c > {4 : 2} ALLOW FILTERING"),
                       row(2, 1, map(6, 2)),
                       row(4, 1, map(7, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE b < 3 AND c <= {4 : 2}");

            assertRows(execute("SELECT * FROM %s WHERE b < 3 AND c <= {4 : 2} ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(3, 2, map(4, 1)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c >= {4 : 3} AND c <= {7 : 1}");

            assertRows(execute("SELECT * FROM %s WHERE c >= {5 : 2} AND c <= {7 : 0} ALLOW FILTERING"),
                       row(2, 1, map(6, 2)));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                                 "SELECT * FROM %s WHERE c CONTAINS 2");

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(2, 1, map(6, 2)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS KEY 4 ALLOW FILTERING"),
                       row(1, 2, map(4, 2)),
                       row(3, 2, map(4, 1)));

            assertRows(execute("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS KEY 6 ALLOW FILTERING"),
                       row(2, 1, map(6, 2)));
        });

        // Checks filtering with null
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c = null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c > null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS null ALLOW FILTERING");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c CONTAINS KEY null");
        assertInvalidMessage("Unsupported null value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY null ALLOW FILTERING");

        // Checks filtering with unset
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c = ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c > ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS ? ALLOW FILTERING",
                             unset());
        assertInvalidMessage("Unsupported unset value for column c",
                             "SELECT * FROM %s WHERE c CONTAINS KEY ? ALLOW FILTERING",
                             unset());
    }

    @Test
    public void filteringOnCompactTable() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, 13, 14);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, 23, 24);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, 26, 27);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, 33, 34);

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c > 13"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27),
                       row(31, 32, 33, 34));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c > 13 AND c < 33"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c > 13 AND b < 32"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a = 21 AND c > 13 AND b < 32 ORDER BY b DESC"),
                       row(21, 25, 26, 27),
                       row(21, 22, 23, 24));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a IN (21, 31) AND c > 13 ORDER BY b DESC"),
                       row(31, 32, 33, 34),
                       row(21, 25, 26, 27),
                       row(21, 22, 23, 24));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c > 13 AND d < 34"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c > 13"),
                       row(21, 22, 23, 24),
                       row(21, 25, 26, 27),
                       row(31, 32, 33, 34));
        });

        // with frozen in clustering key
        createTable("CREATE TABLE %s (a int, b int, c frozen<list<int>>, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, list(1, 3), 14);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, list(2, 3), 24);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, list(2, 6), 27);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, list(3, 3), 34);

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c CONTAINS 2"),
                       row(21, 22, list(2, 3), 24),
                       row(21, 25, list(2, 6), 27));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c CONTAINS 2 AND b < 25"),
                       row(21, 22, list(2, 3), 24));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE c CONTAINS 2 AND c CONTAINS 3"),
                       row(21, 22, list(2, 3), 24));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 12 AND c CONTAINS 2 AND d < 27"),
                       row(21, 22, list(2, 3), 24));
        });

        // with frozen in value
        createTable("CREATE TABLE %s (a int, b int, c int, d frozen<list<int>>, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 11, 12, 13, list(1, 4));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 22, 23, list(2, 4));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 21, 25, 25, list(2, 6));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 31, 32, 34, list(3, 4));

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE d CONTAINS 2"),
                       row(21, 22, 23, list(2, 4)),
                       row(21, 25, 25, list(2, 6)));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE d CONTAINS 2 AND b < 25"),
                       row(21, 22, 23, list(2, 4)));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE d CONTAINS 2 AND d CONTAINS 4"),
                       row(21, 22, 23, list(2, 4)));

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 12 AND c < 25 AND d CONTAINS 2"),
                       row(21, 22, 23, list(2, 4)));
        });
    }

    private UntypedResultSet executeFilteringOnly(String statement) throws Throwable
    {
        assertInvalid(statement);
        return execute(statement + " ALLOW FILTERING");
    }

    @Test
    public void testFilteringWithCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, cnt counter, PRIMARY KEY (a, b, c))" + CompactStorageSplit1Test.compactOption);

        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 14L, 11, 12, 13);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 21, 22, 23);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 27L, 21, 25, 26);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 34L, 31, 32, 33);
        execute("UPDATE %s SET cnt = cnt + ? WHERE a = ? AND b = ? AND c = ?", 24L, 41, 42, 43);

        beforeAndAfterFlush(() -> {

            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt = 24"),
                       row(21, 22, 23, 24L),
                       row(41, 42, 43, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 22 AND cnt = 24"),
                       row(41, 42, 43, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND b < 25 AND cnt = 24"),
                       row(21, 22, 23, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE b > 10 AND c < 25 AND cnt = 24"),
                       row(21, 22, 23, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE a = 21 AND b > 10 AND cnt > 23 ORDER BY b DESC"),
                       row(21, 25, 26, 27L),
                       row(21, 22, 23, 24L));
            assertRows(executeFilteringOnly("SELECT * FROM %s WHERE cnt > 20 AND cnt < 30"),
                       row(21, 22, 23, 24L),
                       row(21, 25, 26, 27L),
                       row(41, 42, 43, 24L));
        });
    }

    /**
     * Check select with and without compact storage, with different column
     * order. See CASSANDRA-10988
     */
    @Test
    public void testClusteringOrderWithSlice() throws Throwable
    {
        final String compactOption = " WITH COMPACT STORAGE AND";

        // non-compound, ASC order
        createTable("CREATE TABLE %s (a text, b int, PRIMARY KEY (a, b)) " +
                    compactOption +
                    " CLUSTERING ORDER BY (b ASC)");

        execute("INSERT INTO %s (a, b) VALUES ('a', 2)");
        execute("INSERT INTO %s (a, b) VALUES ('a', 3)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 2),
                   row("a", 3));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b DESC"),
                   row("a", 3),
                   row("a", 2));

        // non-compound, DESC order
        createTable("CREATE TABLE %s (a text, b int, PRIMARY KEY (a, b))" +
                    compactOption +
                    " CLUSTERING ORDER BY (b DESC)");

        execute("INSERT INTO %s (a, b) VALUES ('a', 2)");
        execute("INSERT INTO %s (a, b) VALUES ('a', 3)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 3),
                   row("a", 2));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2),
                   row("a", 3));

        // compound, first column DESC order
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b, c)) " +
                    compactOption +
                    " CLUSTERING ORDER BY (b DESC)"
        );

        execute("INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 3, 5),
                   row("a", 2, 4));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2, 4),
                   row("a", 3, 5));

        // compound, mixed order
        createTable("CREATE TABLE %s (a text, b int, c int, PRIMARY KEY (a, b, c)) " +
                    compactOption +
                    " CLUSTERING ORDER BY (b ASC, c DESC)"
        );

        execute("INSERT INTO %s (a, b, c) VALUES ('a', 2, 4)");
        execute("INSERT INTO %s (a, b, c) VALUES ('a', 3, 5)");
        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0"),
                   row("a", 2, 4),
                   row("a", 3, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = 'a' AND b > 0 ORDER BY b ASC"),
                   row("a", 2, 4),
                   row("a", 3, 5));
    }


    @Test
    public void testEmptyRestrictionValue() throws Throwable
    {
        for (String options : new String[]{ "", " WITH COMPACT STORAGE" })
        {
            createTable("CREATE TABLE %s (pk blob, c blob, v blob, PRIMARY KEY ((pk), c))" + options);
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"), bytes("2"), bytes("2"));

            beforeAndAfterFlush(() -> {

                assertInvalidMessage("Key may not be empty", "SELECT * FROM %s WHERE pk = text_as_blob('');");
                assertInvalidMessage("Key may not be empty", "SELECT * FROM %s WHERE pk IN (text_as_blob(''), text_as_blob('1'));");

                assertInvalidMessage("Key may not be empty",
                                     "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                     EMPTY_BYTE_BUFFER, bytes("2"), bytes("2"));

                // Test clustering columns restrictions
                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c = text_as_blob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) = (text_as_blob(''));"));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c IN (text_as_blob(''), text_as_blob('1'));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) IN ((text_as_blob('')), (text_as_blob('1')));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('');"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) > (text_as_blob(''));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('');"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) >= (text_as_blob(''));"),
                           row(bytes("foo123"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), bytes("2"), bytes("2")));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c <= text_as_blob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) <= (text_as_blob(''));"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c < text_as_blob('');"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) < (text_as_blob(''));"));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('') AND c < text_as_blob('');"));
            });

            if (options.contains("COMPACT"))
            {
                assertInvalidMessage("Invalid empty or null value for column c",
                                     "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                     bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4"));
            }
            else
            {
                execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                        bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4"));

                beforeAndAfterFlush(() -> {
                    assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c = text_as_blob('');"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) = (text_as_blob(''));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c IN (text_as_blob(''), text_as_blob('1'));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) IN ((text_as_blob('')), (text_as_blob('1')));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('');"),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) > (text_as_blob(''));"),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('');"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) >= (text_as_blob(''));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")),
                               row(bytes("foo123"), bytes("1"), bytes("1")),
                               row(bytes("foo123"), bytes("2"), bytes("2")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c <= text_as_blob('');"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) <= (text_as_blob(''));"),
                               row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("4")));

                    assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c < text_as_blob('');"));

                    assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c) < (text_as_blob(''));"));

                    assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('') AND c < text_as_blob('');"));
                });
            }

            // Test restrictions on non-primary key value
            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND v = text_as_blob('') ALLOW FILTERING;"));

            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"), bytes("3"), EMPTY_BYTE_BUFFER);

            beforeAndAfterFlush(() -> {
                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND v = text_as_blob('') ALLOW FILTERING;"),
                           row(bytes("foo123"), bytes("3"), EMPTY_BYTE_BUFFER));
            });
        }
    }

    @Test
    public void testEmptyRestrictionValueWithMultipleClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))" + CompactStorageSplit1Test.compactOption);
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"), bytes("1"));
        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("2"), bytes("2"));

        beforeAndAfterFlush(() -> {

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 = text_as_blob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) = (text_as_blob('1'), text_as_blob(''));"));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 IN (text_as_blob(''), text_as_blob('1')) AND c2 = text_as_blob('1');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 IN (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) IN ((text_as_blob(''), text_as_blob('1')), (text_as_blob('1'), text_as_blob('1')));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 > text_as_blob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 > text_as_blob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) > (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 >= text_as_blob('');"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 <= text_as_blob('');"));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) <= (text_as_blob('1'), text_as_blob(''));"));
        });

        execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4"));

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('') AND c2 = text_as_blob('1');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) = (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 IN (text_as_blob(''), text_as_blob('1')) AND c2 = text_as_blob('1');"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) IN ((text_as_blob(''), text_as_blob('1')), (text_as_blob('1'), text_as_blob('1')));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) > (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) >= (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")),
                       row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                       row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")));

            assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) <= (text_as_blob(''), text_as_blob('1'));"),
                       row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

            assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) < (text_as_blob(''), text_as_blob('1'));"));
        });
    }

    @Test
    public void testEmptyRestrictionValueWithOrderBy() throws Throwable
    {
        for (String options : new String[]{ " WITH COMPACT STORAGE",
                                            " WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c DESC)" })
        {
            String orderingClause = options.contains("ORDER") ? "" : "ORDER BY c DESC";

            createTable("CREATE TABLE %s (pk blob, c blob, v blob, PRIMARY KEY ((pk), c))" + options);
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"),
                    bytes("1"),
                    bytes("1"));
            execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                    bytes("foo123"),
                    bytes("2"),
                    bytes("2"));

            beforeAndAfterFlush(() -> {

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c > text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c >= text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1")));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c < text_as_blob('')" + orderingClause));

                assertEmpty(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c <= text_as_blob('')" + orderingClause));
            });

            assertInvalidMessage("Invalid empty or null value for column c",
                                 "INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)",
                                 bytes("foo123"),
                                 EMPTY_BYTE_BUFFER,
                                 bytes("4"));
        }
    }

    @Test
    public void testEmptyRestrictionValueWithMultipleClusteringColumnsAndOrderBy() throws Throwable
    {
        for (String options : new String[]{ " WITH COMPACT STORAGE",
                                            " WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c1 DESC, c2 DESC)" })
        {
            String orderingClause = options.contains("ORDER") ? "" : "ORDER BY c1 DESC, c2 DESC";

            createTable("CREATE TABLE %s (pk blob, c1 blob, c2 blob, v blob, PRIMARY KEY (pk, c1, c2))" + options);
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("1"), bytes("1"));
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)", bytes("foo123"), bytes("1"), bytes("2"), bytes("2"));

            beforeAndAfterFlush(() -> {

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 > text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 > text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) > (text_as_blob(''), text_as_blob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 = text_as_blob('1') AND c2 >= text_as_blob('')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));
            });

            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?)",
                    bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4"));

            beforeAndAfterFlush(() -> {

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND c1 IN (text_as_blob(''), text_as_blob('1')) AND c2 = text_as_blob('1')" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) IN ((text_as_blob(''), text_as_blob('1')), (text_as_blob('1'), text_as_blob('1')))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) > (text_as_blob(''), text_as_blob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")));

                assertRows(execute("SELECT * FROM %s WHERE pk = text_as_blob('foo123') AND (c1, c2) >= (text_as_blob(''), text_as_blob('1'))" + orderingClause),
                           row(bytes("foo123"), bytes("1"), bytes("2"), bytes("2")),
                           row(bytes("foo123"), bytes("1"), bytes("1"), bytes("1")),
                           row(bytes("foo123"), EMPTY_BYTE_BUFFER, bytes("1"), bytes("4")));
            });
        }
    }

    /**
     * UpdateTest
     */
    @Test
    public void testUpdate() throws Throwable
    {
        testUpdate(false);
        testUpdate(true);
    }

    private void testUpdate(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                    "clustering_1 int," +
                    "value int," +
                    " PRIMARY KEY (partitionKey, clustering_1))" + CompactStorageSplit1Test.compactOption);

        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 2, 2)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (0, 3, 3)");
        execute("INSERT INTO %s (partitionKey, clustering_1, value) VALUES (1, 0, 4)");

        flush(forceFlush);

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1);
        flush(forceFlush);
        assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ?",
                           0, 1),
                   row(7));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1) = (?)", 8, 0, 2);
        flush(forceFlush);
        assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ?",
                           0, 2),
                   row(8));

        execute("UPDATE %s SET value = ? WHERE partitionKey IN (?, ?) AND clustering_1 = ?", 9, 0, 1, 0);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ?",
                           0, 1, 0),
                   row(0, 0, 9),
                   row(1, 0, 9));

        execute("UPDATE %s SET value = ? WHERE partitionKey IN ? AND clustering_1 = ?", 19, Arrays.asList(0, 1), 0);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey IN ? AND clustering_1 = ?",
                           Arrays.asList(0, 1), 0),
                   row(0, 0, 19),
                   row(1, 0, 19));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 IN (?, ?)", 10, 0, 1, 0);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?)",
                           0, 1, 0),
                   row(0, 0, 10),
                   row(0, 1, 10));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))", 20, 0, 0, 1);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                           0, 0, 1),
                   row(0, 0, 20),
                   row(0, 1, 20));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ?", null, 0, 0);
        flush(forceFlush);

        if (isEmpty(CompactStorageSplit1Test.compactOption))
        {
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                               0, 0, 1),
                       row(0, 0, null),
                       row(0, 1, 20));
        }
        else
        {
            assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1) IN ((?), (?))",
                               0, 0, 1),
                       row(0, 1, 20));
        }

        // test invalid queries

        // missing primary key element
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "UPDATE %s SET value = ? WHERE clustering_1 = ? ", 7, 1);

        assertInvalidMessage("Some clustering keys are missing: clustering_1",
                             "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0);

        assertInvalidMessage("Some clustering keys are missing: clustering_1",
                             "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0);

        // token function
        assertInvalidMessage("The token function cannot be used in WHERE clauses for UPDATE statements",
                             "UPDATE %s SET value = ? WHERE token(partitionKey) = token(?) AND clustering_1 = ?",
                             7, 0, 1);

        // multiple time the same value
        assertInvalidSyntax("UPDATE %s SET value = ?, value = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1);

        // multiple time same primary key element in WHERE clause
        assertInvalidMessage("clustering_1 cannot be restricted by more than one relation if it includes an Equal",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_1 = ?", 7, 0, 1, 1);

        // Undefined column names
        assertInvalidMessage("Undefined column name value1",
                             "UPDATE %s SET value1 = ? WHERE partitionKey = ? AND clustering_1 = ?", 7, 0, 1);

        assertInvalidMessage("Undefined column name partitionkey1",
                             "UPDATE %s SET value = ? WHERE partitionKey1 = ? AND clustering_1 = ?", 7, 0, 1);

        assertInvalidMessage("Undefined column name clustering_3",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_3 = ?", 7, 0, 1);

        // Invalid operator in the where clause
        assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function)",
                             "UPDATE %s SET value = ? WHERE partitionKey > ? AND clustering_1 = ?", 7, 0, 1);

        assertInvalidMessage("Cannot use UPDATE with CONTAINS",
                             "UPDATE %s SET value = ? WHERE partitionKey CONTAINS ? AND clustering_1 = ?", 7, 0, 1);

        assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND value = ?", 7, 0, 1, 3);

        assertInvalidMessage("Slice restrictions are not supported on the clustering columns in UPDATE statements",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 > ?", 7, 0, 1);
    }

    @Test
    public void testUpdateWithTwoClusteringColumns() throws Throwable
    {
        testUpdateWithTwoClusteringColumns(false);
        testUpdateWithTwoClusteringColumns(true);
    }

    private void testUpdateWithTwoClusteringColumns(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey int," +
                    "clustering_1 int," +
                    "clustering_2 int," +
                    "value int," +
                    " PRIMARY KEY (partitionKey, clustering_1, clustering_2))" + CompactStorageSplit1Test.compactOption);

        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 1, 1)");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 2, 2)");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 0, 3, 3)");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 1, 1, 4)");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (0, 1, 2, 5)");
        execute("INSERT INTO %s (partitionKey, clustering_1, clustering_2, value) VALUES (1, 0, 0, 6)");
        flush(forceFlush);

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);
        flush(forceFlush);
        assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 1),
                   row(7));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) = (?, ?)", 8, 0, 1, 2);
        flush(forceFlush);
        assertRows(execute("SELECT value FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 2),
                   row(8));

        execute("UPDATE %s SET value = ? WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 9, 0, 1, 0, 0);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 0, 0),
                   row(0, 0, 0, 9),
                   row(1, 0, 0, 9));

        execute("UPDATE %s SET value = ? WHERE partitionKey IN ? AND clustering_1 = ? AND clustering_2 = ?", 9, Arrays.asList(0, 1), 0, 0);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey IN ? AND clustering_1 = ? AND clustering_2 = ?",
                           Arrays.asList(0, 1), 0, 0),
                   row(0, 0, 0, 9),
                   row(1, 0, 0, 9));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)", 12, 0, 1, 1, 2);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 IN (?, ?)",
                           0, 1, 1, 2),
                   row(0, 1, 1, 12),
                   row(0, 1, 2, 12));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND clustering_2 IN (?, ?)", 10, 0, 1, 0, 1, 2);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND clustering_1 IN (?, ?) AND clustering_2 IN (?, ?)",
                           0, 1, 0, 1, 2),
                   row(0, 0, 1, 10),
                   row(0, 0, 2, 10),
                   row(0, 1, 1, 10),
                   row(0, 1, 2, 10));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))", 20, 0, 0, 2, 1, 2);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))",
                           0, 0, 2, 1, 2),
                   row(0, 0, 2, 20),
                   row(0, 1, 2, 20));

        execute("UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", null, 0, 0, 2);
        flush(forceFlush);


        assertRows(execute("SELECT * FROM %s WHERE partitionKey = ? AND (clustering_1, clustering_2) IN ((?, ?), (?, ?))",
                           0, 0, 2, 1, 2),
                   row(0, 1, 2, 20));


        // test invalid queries

        // missing primary key element
        assertInvalidMessage("Some partition key parts are missing: partitionkey",
                             "UPDATE %s SET value = ? WHERE clustering_1 = ? AND clustering_2 = ?", 7, 1, 1);

        String errorMsg = "PRIMARY KEY column \"clustering_2\" cannot be restricted as preceding column \"clustering_1\" is not restricted";

        assertInvalidMessage(errorMsg,
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_2 = ?", 7, 0, 1);

        assertInvalidMessage("Some clustering keys are missing: clustering_1, clustering_2",
                             "UPDATE %s SET value = ? WHERE partitionKey = ?", 7, 0);

        // token function
        assertInvalidMessage("The token function cannot be used in WHERE clauses for UPDATE statements",
                             "UPDATE %s SET value = ? WHERE token(partitionKey) = token(?) AND clustering_1 = ? AND clustering_2 = ?",
                             7, 0, 1, 1);

        // multiple time the same value
        assertInvalidSyntax("UPDATE %s SET value = ?, value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

        // multiple time same primary key element in WHERE clause
        assertInvalidMessage("clustering_1 cannot be restricted by more than one relation if it includes an Equal",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND clustering_1 = ?", 7, 0, 1, 1, 1);

        // Undefined column names
        assertInvalidMessage("Undefined column name value1",
                             "UPDATE %s SET value1 = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

        assertInvalidMessage("Undefined column name partitionkey1",
                             "UPDATE %s SET value = ? WHERE partitionKey1 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

        assertInvalidMessage("Undefined column name clustering_3",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_3 = ?", 7, 0, 1, 1);

        // Invalid operator in the where clause
        assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function)",
                             "UPDATE %s SET value = ? WHERE partitionKey > ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

        assertInvalidMessage("Cannot use UPDATE with CONTAINS",
                             "UPDATE %s SET value = ? WHERE partitionKey CONTAINS ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 1, 1);

        assertInvalidMessage("Non PRIMARY KEY columns found in where clause: value",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 = ? AND clustering_2 = ? AND value = ?", 7, 0, 1, 1, 3);

        assertInvalidMessage("Slice restrictions are not supported on the clustering columns in UPDATE statements",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND clustering_1 > ?", 7, 0, 1);

        assertInvalidMessage("Slice restrictions are not supported on the clustering columns in UPDATE statements",
                             "UPDATE %s SET value = ? WHERE partitionKey = ? AND (clustering_1, clustering_2) > (?, ?)", 7, 0, 1, 1);
    }

    @Test
    public void testUpdateWithMultiplePartitionKeyComponents() throws Throwable
    {
        testUpdateWithMultiplePartitionKeyComponents(false);
        testUpdateWithMultiplePartitionKeyComponents(true);
    }

    public void testUpdateWithMultiplePartitionKeyComponents(boolean forceFlush) throws Throwable
    {
        createTable("CREATE TABLE %s (partitionKey_1 int," +
                    "partitionKey_2 int," +
                    "clustering_1 int," +
                    "clustering_2 int," +
                    "value int," +
                    " PRIMARY KEY ((partitionKey_1, partitionKey_2), clustering_1, clustering_2))" + CompactStorageSplit1Test.compactOption);

        execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 0, 0, 0, 0)");
        execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 1, 0, 1, 1)");
        execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (0, 1, 1, 1, 2)");
        execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (1, 0, 0, 1, 3)");
        execute("INSERT INTO %s (partitionKey_1, partitionKey_2, clustering_1, clustering_2, value) VALUES (1, 1, 0, 1, 3)");
        flush(forceFlush);

        execute("UPDATE %s SET value = ? WHERE partitionKey_1 = ? AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 0, 0, 0, 0);
        flush(forceFlush);
        assertRows(execute("SELECT value FROM %s WHERE partitionKey_1 = ? AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 0, 0, 0),
                   row(7));

        execute("UPDATE %s SET value = ? WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?", 9, 0, 1, 1, 0, 1);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 = ? AND clustering_1 = ? AND clustering_2 = ?",
                           0, 1, 1, 0, 1),
                   row(0, 1, 0, 1, 9),
                   row(1, 1, 0, 1, 9));

        execute("UPDATE %s SET value = ? WHERE partitionKey_1 IN (?, ?) AND partitionKey_2 IN (?, ?) AND clustering_1 = ? AND clustering_2 = ?", 10, 0, 1, 0, 1, 0, 1);
        flush(forceFlush);
        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, 0, 0, 7),
                   row(0, 0, 0, 1, 10),
                   row(0, 1, 0, 1, 10),
                   row(0, 1, 1, 1, 2),
                   row(1, 0, 0, 1, 10),
                   row(1, 1, 0, 1, 10));

        // missing primary key element
        assertInvalidMessage("Some partition key parts are missing: partitionkey_2",
                             "UPDATE %s SET value = ? WHERE partitionKey_1 = ? AND clustering_1 = ? AND clustering_2 = ?", 7, 1, 1);
    }

    @Test
    public void testCfmCompactStorageCQL()
    {
        String keyspace = "cql_test_keyspace_compact";
        String table = "test_table_compact";

        TableMetadata.Builder metadata =
        TableMetadata.builder(keyspace, table)
                     .flags(EnumSet.of(TableMetadata.Flag.DENSE))
                     .addPartitionKeyColumn("pk1", IntegerType.instance)
                     .addPartitionKeyColumn("pk2", AsciiType.instance)
                     .addClusteringColumn("ck1", ReversedType.getInstance(IntegerType.instance))
                     .addClusteringColumn("ck2", IntegerType.instance)
                     .addRegularColumn("reg", IntegerType.instance);

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), metadata);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), cfs.keyspace.getMetadata());
        String expected = "CREATE TABLE IF NOT EXISTS cql_test_keyspace_compact.test_table_compact (\n" +
                          "    pk1 varint,\n" +
                          "    pk2 ascii,\n" +
                          "    ck1 varint,\n" +
                          "    ck2 varint,\n" +
                          "    reg varint,\n" +
                          "    PRIMARY KEY ((pk1, pk2), ck1, ck2)\n" +
                          ") WITH COMPACT STORAGE\n" +
                          "    AND ID = " + cfs.metadata.id + "\n" +
                          "    AND CLUSTERING ORDER BY (ck1 DESC, ck2 ASC)";
        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    @Test
    public void testCfmCounterCQL()
    {
        String keyspace = "cql_test_keyspace_counter";
        String table = "test_table_counter";

        TableMetadata.Builder metadata;
        metadata = TableMetadata.builder(keyspace, table)
                                .flags(EnumSet.of(TableMetadata.Flag.DENSE,
                                       TableMetadata.Flag.COUNTER))
                                .isCounter(true)
                                .addPartitionKeyColumn("pk1", IntegerType.instance)
                                .addPartitionKeyColumn("pk2", AsciiType.instance)
                                .addClusteringColumn("ck1", ReversedType.getInstance(IntegerType.instance))
                                .addClusteringColumn("ck2", IntegerType.instance)
                                .addRegularColumn("cnt", CounterColumnType.instance);

        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), metadata);

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), cfs.keyspace.getMetadata());
        String expected = "CREATE TABLE IF NOT EXISTS cql_test_keyspace_counter.test_table_counter (\n" +
                          "    pk1 varint,\n" +
                          "    pk2 ascii,\n" +
                          "    ck1 varint,\n" +
                          "    ck2 varint,\n" +
                          "    cnt counter,\n" +
                          "    PRIMARY KEY ((pk1, pk2), ck1, ck2)\n" +
                          ") WITH COMPACT STORAGE\n" +
                          "    AND ID = " + cfs.metadata.id + "\n" +
                          "    AND CLUSTERING ORDER BY (ck1 DESC, ck2 ASC)";
        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    @Test
    public void testDenseTable() throws Throwable
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint PRIMARY KEY," +
                                       "reg1 int)" +
                                       " WITH COMPACT STORAGE");

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), cfs.keyspace.getMetadata());
        String expected = "CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
                        "    pk1 varint,\n" +
                        "    reg1 int,\n" +
                        "    PRIMARY KEY (pk1)\n" +
                        ") WITH COMPACT STORAGE\n" +
                        "    AND ID = " + cfs.metadata.id + "\n";

        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    @Test
    public void testStaticCompactTable()
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint PRIMARY KEY," +
                                       "reg1 int," +
                                       "reg2 int)" +
                                       " WITH COMPACT STORAGE");

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);
        assertTrue(SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), cfs.keyspace.getMetadata()).contains(
        "CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
        "    pk1 varint,\n" +
        "    reg1 int,\n" +
        "    reg2 int,\n" +
        "    PRIMARY KEY (pk1)\n" +
        ") WITH COMPACT STORAGE\n" +
        "    AND ID = " + cfs.metadata.id));
    }

    @Test
    public void testStaticCompactWithCounters()
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint PRIMARY KEY," +
                                       "reg1 counter," +
                                       "reg2 counter)" +
                                       " WITH COMPACT STORAGE");


        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), cfs.keyspace.getMetadata());
        String expected = "CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
                          "    pk1 varint,\n" +
                          "    reg1 counter,\n" +
                          "    reg2 counter,\n" +
                          "    PRIMARY KEY (pk1)\n" +
                          ") WITH COMPACT STORAGE\n" +
                          "    AND ID = " + cfs.metadata.id + "\n";
        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    @Test
    public void testDenseCompactTableWithoutRegulars() throws Throwable
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint," +
                                       "ck1 int," +
                                       "PRIMARY KEY (pk1, ck1))" +
                                       " WITH COMPACT STORAGE");

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), cfs.keyspace.getMetadata());
        String expected = "CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
                          "    pk1 varint,\n" +
                          "    ck1 int,\n" +
                          "    PRIMARY KEY (pk1, ck1)\n" +
                          ") WITH COMPACT STORAGE\n" +
                          "    AND ID = " + cfs.metadata.id;
        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    @Test
    public void testCompactDynamic() throws Throwable
    {
        String tableName = createTable("CREATE TABLE IF NOT EXISTS %s (" +
                                       "pk1 varint," +
                                       "ck1 int," +
                                       "reg int," +
                                       "PRIMARY KEY (pk1, ck1))" +
                                       " WITH COMPACT STORAGE");

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);

        String actual = SchemaCQLHelper.getTableMetadataAsCQL(cfs.metadata(), cfs.keyspace.getMetadata());
        String expected = "CREATE TABLE IF NOT EXISTS " + keyspace() + "." + tableName + " (\n" +
                          "    pk1 varint,\n" +
                          "    ck1 int,\n" +
                          "    reg int,\n" +
                          "    PRIMARY KEY (pk1, ck1)\n" +
                          ") WITH COMPACT STORAGE\n" +
                          "    AND ID = " + cfs.metadata.id;

        assertTrue(String.format("Expected\n%s\nto contain\n%s", actual, expected),
                   actual.contains(expected));
    }

    /**
     * PartitionUpdateTest
     */

    @Test
    public void testOperationCountWithCompactTable()
    {
        createTable("CREATE TABLE %s (key text PRIMARY KEY, a int) WITH COMPACT STORAGE");
        TableMetadata cfm = currentTableMetadata();

        PartitionUpdate update = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "key0").add("a", 1)
                                                                                                 .buildUpdate();
        Assert.assertEquals(1, update.operationCount());

        update = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "key0").buildUpdate();
        Assert.assertEquals(0, update.operationCount());
    }

    /**
     * AlterTest
     */

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testAlterWithCompactStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");

        assertInvalidMessage("Undefined column name column1 in table",
                             "ALTER TABLE %s RENAME column1 TO column2");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testAlterWithCompactNonStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name column1 in table",
                             "ALTER TABLE %s RENAME column1 TO column2");

        createTable("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name column1 in table",
                             "ALTER TABLE %s RENAME column1 TO column2");
    }

    /**
     * CreateTest
     */


    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testCreateIndextWithCompactStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name column1 in table",
                             "CREATE INDEX column1_index on %s (column1)");
        assertInvalidMessage("Undefined column name value in table",
                             "CREATE INDEX value_index on %s (value)");
    }

    /**
     * DeleteTest
     */

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testDeleteWithCompactStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");
        testDeleteWithCompactFormat();

        // if column1 is present, hidden column is called column2
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name column2 in table",
                             "DELETE FROM %s WHERE a = 1 AND column2= 1");
        assertInvalidMessage("Undefined column name column2 in table",
                             "DELETE FROM %s WHERE a = 1 AND column2 = 1 AND value1 = 1");
        assertInvalidMessage("Undefined column name column2",
                             "DELETE column2 FROM %s WHERE a = 1");

        // if value is present, hidden column is called value1
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE");
        assertInvalidMessage("Undefined column name value1 in table",
                             "DELETE FROM %s WHERE a = 1 AND value1 = 1");
        assertInvalidMessage("Undefined column name value1 in table",
                             "DELETE FROM %s WHERE a = 1 AND value1 = 1 AND column1 = 1");
        assertInvalidMessage("Undefined column name value1",
                             "DELETE value1 FROM %s WHERE a = 1");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testDeleteWithCompactNonStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 1)");
        assertRows(execute("SELECT a, b FROM %s"),
                   row(1, 1),
                   row(2, 1));
        testDeleteWithCompactFormat();

        createTable("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, v) VALUES (1, 1, 3)");
        execute("INSERT INTO %s (a, b, v) VALUES (2, 1, 4)");
        assertRows(execute("SELECT a, b, v FROM %s"),
                   row(1, 1, 3),
                   row(2, 1, 4));
        testDeleteWithCompactFormat();
    }

    private void testDeleteWithCompactFormat() throws Throwable
    {
        assertInvalidMessage("Undefined column name value in table",
                             "DELETE FROM %s WHERE a = 1 AND value = 1");
        assertInvalidMessage("Undefined column name column1 in table",
                             "DELETE FROM %s WHERE a = 1 AND column1= 1");
        assertInvalidMessage("Undefined column name value in table",
                             "DELETE FROM %s WHERE a = 1 AND value = 1 AND column1 = 1");
        assertInvalidMessage("Undefined column name value",
                             "DELETE value FROM %s WHERE a = 1");
        assertInvalidMessage("Undefined column name column1",
                             "DELETE column1 FROM %s WHERE a = 1");
    }

    /**
     * InsertTest
     */

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testInsertWithCompactStaticFormat() throws Throwable
    {
        testInsertWithCompactTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");

        // if column1 is present, hidden column is called column2
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, column1) VALUES (1, 1, 1, 1)");
        assertInvalidMessage("Undefined column name column2",
                             "INSERT INTO %s (a, b, c, column2) VALUES (1, 1, 1, 1)");

        // if value is present, hidden column is called value1
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, value) VALUES (1, 1, 1, 1)");
        assertInvalidMessage("Undefined column name value1",
                             "INSERT INTO %s (a, b, c, value1) VALUES (1, 1, 1, 1)");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testInsertWithCompactNonStaticFormat() throws Throwable
    {
        testInsertWithCompactTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        testInsertWithCompactTable("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
    }

    private void testInsertWithCompactTable(String tableQuery) throws Throwable
    {
        createTable(tableQuery);

        // pass correct types to the hidden columns
        assertInvalidMessage("Undefined column name column1",
                             "INSERT INTO %s (a, b, column1) VALUES (?, ?, ?)",
                             1, 1, 1, ByteBufferUtil.bytes('a'));
        assertInvalidMessage("Undefined column name value",
                             "INSERT INTO %s (a, b, value) VALUES (?, ?, ?)",
                             1, 1, 1, ByteBufferUtil.bytes('a'));
        assertInvalidMessage("Undefined column name column1",
                             "INSERT INTO %s (a, b, column1, value) VALUES (?, ?, ?, ?)",
                             1, 1, 1, ByteBufferUtil.bytes('a'), ByteBufferUtil.bytes('b'));
        assertInvalidMessage("Undefined column name value",
                             "INSERT INTO %s (a, b, value, column1) VALUES (?, ?, ?, ?)",
                             1, 1, 1, ByteBufferUtil.bytes('a'), ByteBufferUtil.bytes('b'));

        // pass incorrect types to the hidden columns
        assertInvalidMessage("Undefined column name value",
                             "INSERT INTO %s (a, b, value) VALUES (?, ?, ?)",
                             1, 1, 1, 1);
        assertInvalidMessage("Undefined column name column1",
                             "INSERT INTO %s (a, b, column1) VALUES (?, ?, ?)",
                             1, 1, 1, 1);
        assertEmpty(execute("SELECT * FROM %s"));

        // pass null to the hidden columns
        assertInvalidMessage("Undefined column name value",
                             "INSERT INTO %s (a, b, value) VALUES (?, ?, ?)",
                             1, 1, null);
        assertInvalidMessage("Undefined column name column1",
                             "INSERT INTO %s (a, b, column1) VALUES (?, ?, ?)",
                             1, 1, null);
    }

    /**
     * SelectTest
     */

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testSelectWithCompactStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (a, b, c) VALUES (2, 1, 1)");
        assertRows(execute("SELECT a, b, c FROM %s"),
                   row(1, 1, 1),
                   row(2, 1, 1));
        testSelectWithCompactFormat();

        // if column column1 is present, hidden column is called column2
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, column1) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (a, b, c, column1) VALUES (2, 1, 1, 2)");
        assertRows(execute("SELECT a, b, c, column1 FROM %s"),
                   row(1, 1, 1, 1),
                   row(2, 1, 1, 2));
        assertInvalidMessage("Undefined column name column2 in table",
                             "SELECT a, column2, value FROM %s");

        // if column value is present, hidden column is called value1
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, value) VALUES (1, 1, 1, 1)");
        execute("INSERT INTO %s (a, b, c, value) VALUES (2, 1, 1, 2)");
        assertRows(execute("SELECT a, b, c, value FROM %s"),
                   row(1, 1, 1, 1),
                   row(2, 1, 1, 2));
        assertInvalidMessage("Undefined column name value1 in table",
                             "SELECT a, value1, value FROM %s");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testSelectWithCompactNonStaticFormat() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b) VALUES (1, 1)");
        execute("INSERT INTO %s (a, b) VALUES (2, 1)");
        assertRows(execute("SELECT a, b FROM %s"),
                   row(1, 1),
                   row(2, 1));
        testSelectWithCompactFormat();

        createTable("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, v) VALUES (1, 1, 3)");
        execute("INSERT INTO %s (a, b, v) VALUES (2, 1, 4)");
        assertRows(execute("SELECT a, b, v FROM %s"),
                   row(1, 1, 3),
                   row(2, 1, 4));
        testSelectWithCompactFormat();
    }

    private void testSelectWithCompactFormat() throws Throwable
    {
        assertInvalidMessage("Undefined column name column1 in table",
                             "SELECT column1 FROM %s");
        assertInvalidMessage("Undefined column name value in table",
                             "SELECT value FROM %s");
        assertInvalidMessage("Undefined column name value in table",
                             "SELECT value, column1 FROM %s");
        assertInvalid("Undefined column name column1 in table ('column1 = NULL')",
                      "SELECT * FROM %s WHERE column1 = null ALLOW FILTERING");
        assertInvalid("Undefined column name value in table ('value = NULL')",
                      "SELECT * FROM %s WHERE value = null ALLOW FILTERING");
        assertInvalidMessage("Undefined column name column1 in table",
                             "SELECT WRITETIME(column1) FROM %s");
        assertInvalidMessage("Undefined column name value in table",
                             "SELECT WRITETIME(value) FROM %s");
    }

    /**
     * UpdateTest
     */

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testUpdateWithCompactStaticFormat() throws Throwable
    {
        testUpdateWithCompactFormat("CREATE TABLE %s (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE");

        assertInvalidMessage("Undefined column name column1 in table",
                             "UPDATE %s SET b = 1 WHERE column1 = ?",
                             ByteBufferUtil.bytes('a'));
        assertInvalidMessage("Undefined column name value in table",
                             "UPDATE %s SET b = 1 WHERE value = ?",
                             ByteBufferUtil.bytes('a'));

        // if column1 is present, hidden column is called column2
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, column1 int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, column1) VALUES (1, 1, 1, 1)");
        execute("UPDATE %s SET column1 = 6 WHERE a = 1");
        assertInvalidMessage("Undefined column name column2", "UPDATE %s SET column2 = 6 WHERE a = 0");
        assertInvalidMessage("Undefined column name value", "UPDATE %s SET value = 6 WHERE a = 0");

        // if value is present, hidden column is called value1
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c int, value int) WITH COMPACT STORAGE");
        execute("INSERT INTO %s (a, b, c, value) VALUES (1, 1, 1, 1)");
        execute("UPDATE %s SET value = 6 WHERE a = 1");
        assertInvalidMessage("Undefined column name column1", "UPDATE %s SET column1 = 6 WHERE a = 1");
        assertInvalidMessage("Undefined column name value1", "UPDATE %s SET value1 = 6 WHERE a = 1");
    }

    /**
     * Test for CASSANDRA-13917
     */
    @Test
    public void testUpdateWithCompactNonStaticFormat() throws Throwable
    {
        testUpdateWithCompactFormat("CREATE TABLE %s (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        testUpdateWithCompactFormat("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
    }

    private void testUpdateWithCompactFormat(String tableQuery) throws Throwable
    {
        createTable(tableQuery);
        // pass correct types to hidden columns
        assertInvalidMessage("Undefined column name column1",
                             "UPDATE %s SET column1 = ? WHERE a = 0",
                             ByteBufferUtil.bytes('a'));
        assertInvalidMessage("Undefined column name value",
                             "UPDATE %s SET value = ? WHERE a = 0",
                             ByteBufferUtil.bytes('a'));

        // pass incorrect types to hidden columns
        assertInvalidMessage("Undefined column name column1", "UPDATE %s SET column1 = 6 WHERE a = 0");
        assertInvalidMessage("Undefined column name value", "UPDATE %s SET value = 6 WHERE a = 0");
    }
}
