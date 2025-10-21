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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.cql3.statements.SelectOptions;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.index.CustomIndexTest;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.restrictions.StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE;
import static org.apache.cassandra.db.filter.IndexHints.CONFLICTING_INDEXES_ERROR;
import static org.apache.cassandra.db.filter.IndexHints.MISSING_INDEX_ERROR;
import static org.apache.cassandra.db.filter.IndexHints.TOO_MANY_INDEXES_ERROR;
import static org.apache.cassandra.db.filter.IndexHints.WRONG_KEYSPACE_ERROR;

/**
 * Tests the {@link IndexHints} independently of the specific underlying index implementation details.
 * </p>
 * Regarding its effects on the {@link Index.QueryPlan} that is generated for the query:
 * <ul>
 *    <li>Included indexes should be included in the {@link Index.QueryPlan} for the query or fail.</li>
 *    <li>Excluded indexes should not be included in the {@link Index.QueryPlan} for the query.</li>
 * </ul>
 */
public class IndexHintsTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        // Set the messaging version that adds support for the new index hints before starting the server
        CQLTester.setUpClass();
        CQLTester.enableCoordinatorExecution();
        DatabaseDescriptor.setPrioritizeSAIOverLegacyIndex(true);
    }

    /**
     * Test parsing and validation of index hints in {@code SELECT} queries.
     */
    @Test
    public void testParseAndValidate()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int, c int)");
        String query = "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 3 ALLOW FILTERING ";

        // valid queries without index hints
        execute(query);
        execute(query + "WITH included_indexes={}");
        execute(query + "WITH excluded_indexes={}");
        execute(query + "WITH included_indexes={} AND excluded_indexes={}");

        // index hints with unparseable properties
        assertInvalidThrowMessage("mismatched input",
                                  SyntaxException.class,
                                  query + "WITH included_indexes={'a': 'b'}");

        // invalid queries with unknown index (no index has been created yet)
        String missingIndexError = String.format(MISSING_INDEX_ERROR, currentTable(), "idx1");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1,idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes={idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes={idx1,idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1} AND excluded_indexes={idx2}");

        // invalid queries with repeated included or excluded indexes
        assertInvalidThrowMessage(String.format(PropertyDefinitions.MULTIPLE_DEFINITIONS_ERROR, SelectOptions.INCLUDED_INDEXES),
                                  SyntaxException.class,
                                  query + "WITH included_indexes={idx1} AND included_indexes={idx2}");
        assertInvalidThrowMessage(String.format(PropertyDefinitions.MULTIPLE_DEFINITIONS_ERROR, SelectOptions.EXCLUDED_INDEXES),
                                  SyntaxException.class,
                                  query + "WITH excluded_indexes={idx1} AND excluded_indexes={idx2}");
        assertInvalidThrowMessage(String.format(PropertyDefinitions.MULTIPLE_DEFINITIONS_ERROR, SelectOptions.INCLUDED_INDEXES),
                                  SyntaxException.class,
                                  query + "WITH included_indexes={idx1} AND included_indexes={idx2} AND excluded_indexes={idx3}");
        assertInvalidThrowMessage(String.format(PropertyDefinitions.MULTIPLE_DEFINITIONS_ERROR, SelectOptions.EXCLUDED_INDEXES),
                                  SyntaxException.class,
                                  query + "WITH included_indexes={idx1} AND excluded_indexes={idx2} AND excluded_indexes={idx3}");

        // create a single index and test queries with it
        createIndex(String.format("CREATE CUSTOM INDEX idx1 ON %%s(a) USING '%s'", GroupedIndex.class.getName()));
        execute(query + "WITH included_indexes={}");
        execute(query + "WITH included_indexes={idx1}");
        missingIndexError = String.format(MISSING_INDEX_ERROR, currentTable(), "idx2");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx2}");
        execute(query + "WITH excluded_indexes={}");
        execute(query + "WITH excluded_indexes={idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes={idx2}");
        execute(query + "WITH included_indexes={} AND excluded_indexes={}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1} AND excluded_indexes={idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx2} AND excluded_indexes={idx1}");
        assertConflictingHints(query + "WITH included_indexes={idx1} AND excluded_indexes={idx1}", "idx1");

        // create a second index and test queries with both indexes
        createIndex(String.format("CREATE CUSTOM INDEX idx2 ON %%s(b) USING '%s'", GroupedIndex.class.getName()));
        execute(query + "WITH included_indexes={}");
        execute(query + "WITH included_indexes={idx1}");
        execute(query + "WITH included_indexes={idx2}");
        execute(query + "WITH included_indexes={idx1,idx2}");
        execute(query + "WITH included_indexes={idx1,idx1}");
        execute(query + "WITH excluded_indexes={}");
        execute(query + "WITH excluded_indexes={idx1}");
        execute(query + "WITH excluded_indexes={idx2}");
        execute(query + "WITH excluded_indexes={idx1,idx2}");
        execute(query + "WITH excluded_indexes={idx1,idx1}");
        execute(query + "WITH included_indexes={} AND excluded_indexes={}");
        execute(query + "WITH included_indexes={idx1} AND excluded_indexes={idx2}");
        execute(query + "WITH included_indexes={idx2} AND excluded_indexes={idx1}");
        assertConflictingHints(query + "WITH included_indexes={idx1} AND excluded_indexes={idx1}", "idx1");
        assertConflictingHints(query + "WITH included_indexes={idx2} AND excluded_indexes={idx2}", "idx2");
        assertConflictingHints(query + "WITH included_indexes={idx1,idx2} AND excluded_indexes={idx1}", "idx1");
        assertConflictingHints(query + "WITH included_indexes={idx1} AND excluded_indexes={idx1,idx2}", "idx1");
        assertConflictingHints(query + "WITH included_indexes={idx1,idx2} AND excluded_indexes={idx1,idx2}", "idx1,idx2");

        // invalid queries referencing other keyspaces
        String wrongKeyspaceError = String.format(WRONG_KEYSPACE_ERROR, "ks1.idx1");
        assertInvalidThrowMessage(wrongKeyspaceError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={ks1.idx1}");
        assertInvalidThrowMessage(wrongKeyspaceError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes={ks1.idx1}");

        // valid queries with explicit keyspace
        String keyspace = keyspace();
        execute(query + "WITH included_indexes={" + keyspace + ".idx1}");
        execute(query + "WITH included_indexes={" + keyspace + ".idx1, idx2}");
        execute(query + "WITH included_indexes={" + keyspace + ".idx1, " + keyspace + ".idx2}");
        execute(query + "WITH excluded_indexes={" + keyspace + ".idx1}");
        execute(query + "WITH excluded_indexes={" + keyspace + ".idx1, idx2}");
        execute(query + "WITH included_indexes={" + keyspace + ".idx1} AND excluded_indexes={idx2}");
        execute(query + "WITH included_indexes={" + keyspace + ".idx1} AND excluded_indexes={" + keyspace + ".idx2}");
        execute(query + "WITH excluded_indexes={" + keyspace + ".idx1} AND included_indexes={idx2}");
        execute(query + "WITH excluded_indexes={" + keyspace + ".idx1} AND included_indexes={" + keyspace + ".idx2}");

        // valid queries with quoted names
        execute(query + "WITH included_indexes={\"idx1\"}");
        execute(query + "WITH excluded_indexes={\"idx1\", idx2}");
        execute(query + "WITH excluded_indexes={\"idx1\", \"idx2\"}");
        execute(query + "WITH excluded_indexes={\"idx1\"}");
        execute(query + "WITH excluded_indexes={\"idx1\", idx2}");
        execute(query + "WITH excluded_indexes={\"idx1\", \"idx2\"}");
        execute(query + "WITH included_indexes={\"idx1\"} AND excluded_indexes={idx2}");
        execute(query + "WITH included_indexes={\"idx1\"} AND excluded_indexes={\"idx2\"}");
        execute(query + "WITH excluded_indexes={\"idx1\"} AND included_indexes={idx2}");
        execute(query + "WITH excluded_indexes={\"idx1\"} AND included_indexes={\"idx2\"}");
    }

    @Test
    public void testCreate()
    {
        Assertions.assertThat(IndexHints.create(null, null))
                  .isEqualTo(IndexHints.create(indexMetadata(), indexMetadata()))
                  .isSameAs(IndexHints.NONE)
                  .matches(i -> i.included.isEmpty())
                  .matches(i -> i.excluded.isEmpty())
                  .matches(i -> !i.includes("idx1"))
                  .matches(i -> !i.includes("idx2"))
                  .matches(i -> !i.includes("idx3"))
                  .matches(i -> !i.includes("idx4"))
                  .matches(i -> !i.excludes("idx1"))
                  .matches(i -> !i.excludes("idx2"))
                  .matches(i -> !i.excludes("idx3"))
                  .matches(i -> !i.excludes("idx4"));

        Assertions.assertThat(IndexHints.create(indexMetadata("idx1", "idx2"), null))
                  .isEqualTo(IndexHints.create(indexMetadata("idx1", "idx2"), indexMetadata()))
                  .isNotEqualTo(IndexHints.NONE)
                  .matches(i -> i.included.size() == 2)
                  .matches(i -> i.excluded.isEmpty())
                  .matches(i -> i.includes("idx1"))
                  .matches(i -> i.includes("idx2"))
                  .matches(i -> !i.includes("idx3"))
                  .matches(i -> !i.includes("idx4"))
                  .matches(i -> !i.excludes("idx1"))
                  .matches(i -> !i.excludes("idx2"))
                  .matches(i -> !i.excludes("idx3"))
                  .matches(i -> !i.excludes("idx4"));

        Assertions.assertThat(IndexHints.create(null, indexMetadata("idx3", "idx4")))
                  .isEqualTo(IndexHints.create(indexMetadata(), indexMetadata("idx3", "idx4")))
                  .isNotEqualTo(IndexHints.NONE)
                  .matches(i -> i.included.isEmpty())
                  .matches(i -> i.excluded.size() == 2)
                  .matches(i -> !i.includes("idx1"))
                  .matches(i -> !i.includes("idx2"))
                  .matches(i -> !i.includes("idx3"))
                  .matches(i -> !i.includes("idx4"))
                  .matches(i -> !i.excludes("idx1"))
                  .matches(i -> !i.excludes("idx2"))
                  .matches(i -> i.excludes("idx3"))
                  .matches(i -> i.excludes("idx4"));

        Assertions.assertThat(IndexHints.create(indexMetadata("idx1", "idx2"), indexMetadata("idx3", "idx4")))
                  .isNotEqualTo(IndexHints.NONE)
                  .matches(i -> i.included.size() == 2)
                  .matches(i -> i.excluded.size() == 2)
                  .matches(i -> i.includes("idx1"))
                  .matches(i -> i.includes("idx2"))
                  .matches(i -> !i.includes("idx3"))
                  .matches(i -> !i.includes("idx4"))
                  .matches(i -> !i.excludes("idx1"))
                  .matches(i -> !i.excludes("idx2"))
                  .matches(i -> i.excludes("idx3"))
                  .matches(i -> i.excludes("idx4"));
    }

    @Test
    public void testEqualsAndHashCode()
    {
        testEqualsAndHashCode(Pair.create(indexMetadata(), indexMetadata()),
                              Pair.create(indexMetadata("idx1"), indexMetadata()),
                              Pair.create(indexMetadata(), indexMetadata("idx1")),
                              Pair.create(indexMetadata("idx1"), indexMetadata("idx2")),
                              Pair.create(indexMetadata("idx1", "idx2"), indexMetadata()),
                              Pair.create(indexMetadata(), indexMetadata("idx1", "idx2")),
                              Pair.create(indexMetadata("idx1", "idx2"), indexMetadata("idx3", "idx4")));
    }

    @SafeVarargs
    private static void testEqualsAndHashCode(Pair<Set<IndexMetadata>, Set<IndexMetadata>>... dataset)
    {
        for (int i = 0; i < dataset.length; i++)
        {
            for (int j = 0; j < dataset.length; j++)
            {
                if (i != j)
                     assertEqualsAndHashCode(dataset[i], dataset[j]);
            }
        }
    }

    private static void assertEqualsAndHashCode(Pair<Set<IndexMetadata>, Set<IndexMetadata>> one,
                                                Pair<Set<IndexMetadata>, Set<IndexMetadata>> other)
    {
        IndexHints hints = IndexHints.create(one.left, one.right);
        Assertions.assertThat(hints)
                  .isEqualTo(hints)
                  .isNotEqualTo(null)
                  .isNotEqualTo(1);

        IndexHints sameHints = IndexHints.create(one.left, one.right);
        Assertions.assertThat(hints).isEqualTo(sameHints);
        Assertions.assertThat(sameHints).isEqualTo(hints);
        Assertions.assertThat(hints.hashCode()).isEqualTo(sameHints.hashCode());

        IndexHints otherHints = IndexHints.create(other.left, other.right);
        Assertions.assertThat(hints).isNotEqualTo(otherHints);
        Assertions.assertThat(otherHints).isNotEqualTo(hints);
        Assertions.assertThat(hints.hashCode()).isNotEqualTo(otherHints.hashCode());

        Map<IndexHints, Integer> map = new HashMap<>();
        map.put(hints, 1);
        map.put(otherHints, 2);
        Assertions.assertThat(map.get(hints)).isEqualTo(1);
        Assertions.assertThat(map.get(sameHints)).isEqualTo(1);
        Assertions.assertThat(map.get(otherHints)).isEqualTo(2);
    }

    @Test
    public void testToString()
    {
        assertToString(Collections.emptySet(), Collections.emptySet(), "IndexHints{included=, excluded=}");
        assertToString(indexMetadata("idx1"), indexMetadata(), "IndexHints{included=idx1, excluded=}");
        assertToString(indexMetadata(), indexMetadata("idx1"), "IndexHints{included=, excluded=idx1}");
        assertToString(indexMetadata("idx1", "idx2"), indexMetadata(), "IndexHints{included=idx1,idx2, excluded=}");
        assertToString(indexMetadata(), indexMetadata("idx1", "idx2"), "IndexHints{included=, excluded=idx1,idx2}");
        assertToString(indexMetadata("idx1", "idx2"), indexMetadata("idx3", "idx4"), "IndexHints{included=idx1,idx2, excluded=idx3,idx4}");
    }

    private static void assertToString(Set<IndexMetadata> included, Set<IndexMetadata> excluded, String expected)
    {
        IndexHints hints = IndexHints.create(included, excluded);
        Assertions.assertThat(hints.toString()).isEqualTo(expected);
    }

    private static Set<IndexMetadata> indexMetadata(String... names)
    {
        Set<IndexMetadata> indexes = new HashSet<>(names.length);
        for (String name : names)
            indexes.add(IndexMetadata.fromSchemaMetadata(name, IndexMetadata.Kind.CUSTOM, Collections.emptyMap()));
        return indexes;
    }

    /**
     * Test that index hints are considered when generating the CQL string representation of a {@link ReadCommand}.
     */
    @Test
    public void testToCQLString()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");
        createIndex(String.format("CREATE CUSTOM INDEX idx1 ON %%s(a) USING '%s'", GroupedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX idx2 ON %%s(b) USING '%s'", GroupedIndex.class.getName()));

        // without index hints
        String formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0");
        ReadCommand command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .doesNotContain("included_indexes")
                  .doesNotContain("excluded_indexes");

        // with empty hints
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 " +
                                     "WITH included_indexes={} AND excluded_indexes={}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .doesNotContain("included_indexes")
                  .doesNotContain("excluded_indexes");

        // with included indexes only
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 " +
                                     "WITH included_indexes={idx1,idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH included_indexes = {idx1, idx2}")
                  .doesNotContain("excluded_indexes");

        // with excluded indexes only
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 ALLOW FILTERING " +
                                     "WITH excluded_indexes={idx1,idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH excluded_indexes = {idx1, idx2}")
                  .doesNotContain("included_indexes");

        // with both included and excluded indexes
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 ALLOW FILTERING " +
                                     "WITH included_indexes={idx1} AND excluded_indexes={idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH included_indexes = {idx1} AND excluded_indexes = {idx2}");

        // with a single-partition read command
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE k=1 AND a = 0 AND b = 0 ALLOW FILTERING " +
                                     "WITH included_indexes={idx1} AND excluded_indexes={idx2}");
        SinglePartitionReadCommand.Group group = parseReadCommandGroup(formattedQuery);
        Assertions.assertThat(group.queries.get(0).toCQLString())
                  .contains(" WITH included_indexes = {idx1} AND excluded_indexes = {idx2}");
    }

    /**
     * Verify that the index hints in a query get to the {@link ReadCommand} and the {@link Index},
     * even after serialization.
     */
    @Test
    public void testTransport()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int, c int, d int)");
        createIndex(String.format("CREATE CUSTOM INDEX idx1 ON %%s(a) USING '%s'", GroupedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX idx2 ON %%s(b) USING '%s'", GroupedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX idx3 ON %%s(c) USING '%s'", GroupedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX idx4 ON %%s(d) USING '%s'", GroupedIndex.class.getName()));
        IndexMetadata idx1 = getIndex("idx1").getIndexMetadata();
        IndexMetadata idx2 = getIndex("idx2").getIndexMetadata();
        IndexMetadata idx3 = getIndex("idx3").getIndexMetadata();
        IndexMetadata idx4 = getIndex("idx4").getIndexMetadata();
        String query = "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 ALLOW FILTERING ";

        // unspecified hints should be mapped to NONE
        testTransport(query, IndexHints.NONE);
        testTransport(query + "WITH included_indexes={}", IndexHints.NONE);
        testTransport(query + "WITH excluded_indexes={}", IndexHints.NONE);

        // hints with a single index
        testTransport(query + "WITH included_indexes={idx1}", IndexHints.create(indexes(idx1), indexes()));
        testTransport(query + "WITH excluded_indexes={idx1}", IndexHints.create(indexes(), indexes(idx1)));
        testTransport(query + "WITH included_indexes={idx1} AND excluded_indexes={idx2}", IndexHints.create(indexes(idx1), indexes(idx2)));

        // hints with multiple indexes
        testTransport(query + "WITH included_indexes={idx1,idx2}", IndexHints.create(indexes(idx1, idx2), indexes()));
        testTransport(query + "WITH excluded_indexes={idx1,idx2}", IndexHints.create(indexes(), indexes(idx1, idx2)));
        testTransport(query + "WITH included_indexes={idx1,idx2} AND excluded_indexes={idx3,idx4}", IndexHints.create(indexes(idx1, idx2), indexes(idx3, idx4)));
    }

    private void testTransport(String query, IndexHints expectedHints)
    {
        // verify that the hints arrive correctly at the index
        execute(query);
        Assertions.assertThat(GroupedIndex.lastQueryIndexHints).isEqualTo(expectedHints);

        // verify that the hints are correctly parsed and stored in the ReadCommand
        String formattedQuery = formatQuery(query);
        ReadCommand command = parseReadCommand(formattedQuery);
        IndexHints actualHints = command.rowFilter().indexHints;
        Assertions.assertThat(actualHints).isEqualTo(expectedHints);

        // serialize and deserialize the command to check if the hints are preserved...
        try
        {
            // ...with a version that supports index hints
            DataOutputBuffer out = new DataOutputBuffer();
            ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_51);
            Assertions.assertThat(ReadCommand.serializer.serializedSize(command, MessagingService.VERSION_51))
                      .isEqualTo(out.buffer().remaining());
            DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
            command = ReadCommand.serializer.deserialize(in, MessagingService.VERSION_51);
            actualHints = command.rowFilter().indexHints;
            Assertions.assertThat(actualHints).isEqualTo(expectedHints);

            // ...with a version that doesn't support index hints
            out = new DataOutputBuffer();
            if (expectedHints != IndexHints.NONE)
            {
                try
                {
                    ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_50);
                }
                catch (IllegalStateException e)
                {
                    // expected
                    Assertions.assertThat(e)
                              .hasMessageContaining("Unable to serialize index hints with messaging version: " + MessagingService.VERSION_50);
                }
            }
            else
            {
                ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_50);
                Assertions.assertThat(ReadCommand.serializer.serializedSize(command, MessagingService.VERSION_50))
                          .isEqualTo(out.buffer().remaining());
                in = new DataInputBuffer(out.buffer(), true);
                command = ReadCommand.serializer.deserialize(in, MessagingService.VERSION_50);
                actualHints = command.rowFilter().indexHints;
                Assertions.assertThat(actualHints).isEqualTo(IndexHints.NONE);
            }
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testLegacyIndexWithAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1) USING 'legacy_local_table'");
        String idx2 = createIndex("CREATE INDEX idx2 ON %s(v2) USING 'legacy_local_table'");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s ALLOW FILTERING", row1, row2, row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING").selectsAnyOf(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING").selects(idx2);

        // with a single restriction and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx1}", row1).selects(idx1);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx2}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx2}", row2).selects(idx2);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx1}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx2}");

        // with a single restriction and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row1).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row3).selectsNone();

        // with restrictions in two columns (v1 and v2) and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx2}").selects(idx2);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selectsNone();

        // with restrictions in two columns (v1 and v3) and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selectsNone();

        // without restrictions
        assertNonIncludableIndexesError("SELECT * FROM %s ALLOW FILTERING WITH included_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s ALLOW FILTERING WITH excluded_indexes={idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH included_indexes={idx1}");
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH excluded_indexes={idx1}");
    }

    @Test
    public void testLegacyIndexWithoutAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1) USING 'legacy_local_table'");
        String idx2 = createIndex("CREATE INDEX idx2 ON %s(v2) USING 'legacy_local_table'");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s", row1, row2, row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 AND v3=0");

        // with a single restriction and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1}", row1).selects(idx1);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx2}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx2}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH included_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH included_indexes={idx2}");

        // with a single restriction and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx1}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes={idx2}");

        // with restrictions in two columns (v1 and v2) and included indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v3) and included indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx1,idx2}");

        // without restrictions
        assertNonIncludableIndexesError("SELECT * FROM %s WITH included_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WITH excluded_indexes={idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=?");
        prepare("SELECT * FROM %s WHERE v1=? WITH included_indexes={idx1}");
        assertNeedsAllowFiltering(() -> prepare("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx1}"));
    }

    @Test
    public void testSAIWithAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1) USING 'sai'");
        String idx2 = createIndex("CREATE INDEX idx2 ON %s(v2) USING 'sai'");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s ALLOW FILTERING", row1, row2, row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING").selects(idx2);

        // with a single restriction and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx1}", row1).selects(idx1);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx2}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx2}", row2).selects(idx2);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx1}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx2}");

        // with a single restriction and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row1).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row3).selectsNone();

        // with restrictions in two columns (v1 and v2) and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx2}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}").selects(idx1, idx2);

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selectsNone();

        // with restrictions in two columns (v1 and v3) and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selectsNone();

        // with mixed included and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1} AND excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx2} AND excluded_indexes={idx1}").selects(idx2);

        // without restrictions
        assertNonIncludableIndexesError("SELECT * FROM %s ALLOW FILTERING WITH included_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s ALLOW FILTERING WITH excluded_indexes={idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH included_indexes={idx1}");
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH excluded_indexes={idx1}");
    }

    @Test
    public void testSAIWithoutAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1) USING 'sai'");
        String idx2 = createIndex("CREATE INDEX idx2 ON %s(v2) USING 'sai'");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s", row1, row2, row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0").selectsAnyOf(idx1, idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 AND v3=0");

        // with a single restriction and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1}", row1).selects(idx1);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx2}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx2}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH included_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH included_indexes={idx2}");

        // with a single restriction and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx1}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes={idx2}");

        // with restrictions in two columns (v1 and v2) and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx2}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1,idx2}").selects(idx1, idx2);

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v3) and included indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx1,idx2}");

        // with mixed included and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1} AND excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx2} AND excluded_indexes={idx1}");

        // without restrictions
        assertNonIncludableIndexesError("SELECT * FROM %s WITH included_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WITH excluded_indexes={idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=? WITH included_indexes={idx1}");
        assertNeedsAllowFiltering(() -> prepare("SELECT * FROM %s WHERE v1=? WITH excluded_indexes={idx1}"));
    }

    @Test
    public void testLegacy()
    {
        testSingletonIndex("CREATE INDEX %s ON %%s(%s) USING 'legacy_local_table'");
    }

    @Test
    public void testSASI()
    {
        testSingletonIndex("CREATE CUSTOM INDEX %s ON %%s(%s) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
    }

    private void testSingletonIndex(String createIndexQuery)
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");
        String idx1 = createIndex(format(createIndexQuery, "idx1", "v1"));
        String idx2 = createIndex(format(createIndexQuery, "idx2", "v2"));

        String insert = "INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 0 };
        Object[] row2 = new Object[]{ 2, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 0 };
        Object[] row4 = new Object[]{ 4, 1, 1 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0", row1, row2).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0", row1, row3).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING", row1).selectsAnyOf(idx1, idx2);

        // including idx1
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1}", row1, row2).selects(idx1);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1}", row1).selects(idx1);

        // including idx1 and idx2
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1,idx2}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx1,idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1,idx2}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}");

        // excluding idx1
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row1, row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx1}", row1, row3).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row1).selects(idx2);

        // excluding idx1 and idx2
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx1,idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx1,idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx1,idx2}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", row1, row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", row1, row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", row1).selectsNone();
    }

    @Test
    public void testMixedIndexImplementations()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1) USING 'sai'");
        String idx2 = createIndex("CREATE INDEX idx2 ON %s(v2) USING 'sai'");
        String idx3 = createIndex("CREATE INDEX idx3 ON %s(v3) USING 'legacy_local_table'");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING", row3).selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING").selects(idx2);

        // including idx1 (SAI)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx1}", row1).selects(idx1);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx1}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1}");

        // including idx2 (SAI)
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx2}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx2}", row2).selects(idx2);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx2}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx2}").selects(idx1, idx2);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2}").selects(idx2);

        // including idx3 (legacy)
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx3}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx3}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx3}", row3).selects(idx3);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx3}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx3}").selects(idx3); // chooses legacy over SAI
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx3}").selects(idx3); // chooses legacy over SAI

        // including idx1 and idx2 (both SAI)
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}").selects(idx1, idx2);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}");

        // including idx1 and idx3 (SAI and legacy)
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}");

        // including idx2 and idx3 (SAI and legacy)
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx2,idx3}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx2,idx3}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx2,idx3}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2,idx3}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2,idx3}");

        // excluding idx1 (SAI)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row1).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row3).selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx2);

        // excluding idx2 (SAI)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row3).selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx3);

        // excluding idx3 (legacy)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx3}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx3}", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx3}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}").selects(idx2);

        // excluding idx1 and idx2 (both SAI)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", row1).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", row3).selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selects(idx3);

        // excluding idx1 and idx3 (SAI and legacy)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}", row1).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}").selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}").selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}").selects(idx2);

        // excluding idx2 and idx3 (SAI and legacy)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}", row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}").selectsNone();
    }

    @Test
    public void testMultipleIndexesOnSameColumnLegacyAndSASI()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String sasi = createIndex("CREATE CUSTOM INDEX sasi ON %s(v) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
        String legacy = createIndex("CREATE INDEX legacy ON %s(v) USING 'legacy_local_table'");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, 1 };
        Object[] row2 = new Object[]{ 2, 2 };
        Object[] row3 = new Object[]{ 3, 3 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0").selectsAnyOf(legacy, sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=1", row1).selectsAnyOf(legacy, sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v>1", row2, row3).selects(sasi); // legacy doesn't support >

        // including SASI
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH included_indexes={sasi}").selects(sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=1 WITH included_indexes={sasi}", row1).selects(sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v>1 WITH included_indexes={sasi}", row2, row3).selects(sasi);

        // including legacy
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH included_indexes={legacy}").selects(legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=1 WITH included_indexes={legacy}", row1).selects(legacy);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v>1 WITH included_indexes={legacy}"); // legacy doesn't support >

        // excluding SASI
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={sasi}").selects(legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=1 WITH excluded_indexes={sasi}", row1).selects(legacy);
        assertIndexDoesNotSupportOperator("SELECT * FROM %s WHERE v>1 WITH excluded_indexes={sasi}"); // legacy doesn't support >
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v>1 ALLOW FILTERING WITH excluded_indexes={sasi}", row2, row3).selectsNone();

        // excluding legacy
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={legacy}").selects(sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=1 WITH excluded_indexes={legacy}", row1).selects(sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v>1 WITH excluded_indexes={legacy}", row2, row3).selects(sasi); // legacy doesn't support >
    }

    @Test
    public void testMultipleIndexesOnSameColumnLegacyAndSAI()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String sai = createIndex("CREATE INDEX sai ON %s(v) USING 'sai'");
        String legacy = createIndex("CREATE INDEX legacy ON %s(v) USING 'legacy_local_table'");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, 0 };
        Object[] row2 = new Object[]{ 2, 1 };
        execute(insert, row1);
        execute(insert, row2);

        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0", row1).selects(sai);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH included_indexes={sai}", row1).selects(sai);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH included_indexes={legacy}", row1).selects(legacy);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v=0 WITH included_indexes={sai,legacy}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={sai}", row1).selects(legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={legacy}", row1).selects(sai);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={sai,legacy}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 ALLOW FILTERING WITH excluded_indexes={sai,legacy}", row1).selectsNone();
    }

    @Test
    public void testMultipleIndexesOnSameColumnSAIAndSASI()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String sai = createIndex("CREATE INDEX sai ON %s(v) USING 'sai'");
        String sasi = createIndex("CREATE CUSTOM INDEX sasi ON %s(v) USING 'org.apache.cassandra.index.sasi.SASIIndex'");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, 0 };
        Object[] row2 = new Object[]{ 2, 1 };
        execute(insert, row1);
        execute(insert, row2);

        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0", row1).selectsAnyOf(sai, sasi); // SAI and SASI have the same selectivity
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH included_indexes={sai}", row1).selects(sai);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH included_indexes={sasi}", row1).selects(sasi);
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v=0 WITH included_indexes={sai,sasi}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={sai}", row1).selects(sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={sasi}", row1).selects(sai);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={sai,sasi}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 ALLOW FILTERING WITH excluded_indexes={sai,sasi}", row1).selectsNone();
    }

    @Test
    public void testDuplicatedHints()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1) USING 'legacy_local_table'");
        String idx2 = createIndex("CREATE INDEX idx2 ON %s(v2) USING 'sai'");
        String idx3 = createIndex("CREATE CUSTOM INDEX idx3 ON %s(v3) USING 'org.apache.cassandra.index.sasi.SASIIndex'");

        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1,idx1}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx2,idx2}").selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 WITH included_indexes={idx3,idx3}").selects(idx3);

        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx1,idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx2,idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes={idx3,idx3}");

        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx1}").selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx2}").selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx3,idx3}").selectsNone();

        assertConflictingHints("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1} AND excluded_indexes={idx1}", "idx1");
        assertConflictingHints("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx2} AND excluded_indexes={idx2}", "idx2");
        assertConflictingHints("SELECT * FROM %s WHERE v3=0 WITH included_indexes={idx3} AND excluded_indexes={idx3}", "idx3");
    }

    /**
     * Tests that CQL will throw an exception when trying to specify more indexes than can fit in a short.
     */
    @Test
    public void testMaxHints()
    {
        // prepare a set of index names that exceeds the limit
        Set<QualifiedName> indexes = new HashSet<>();
        for (int i = 0; i <= Short.MAX_VALUE; i++)
            indexes.add(new QualifiedName(keyspace(), "idx" + i));

        // test too many included indexes
        Assertions.assertThatThrownBy(() -> IndexHints.fromCQLNames(indexes, null, null, null))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(TOO_MANY_INDEXES_ERROR + indexes.size());

        // test too many excluded indexes
        Assertions.assertThatThrownBy(() -> IndexHints.fromCQLNames(null, indexes, null, null))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(TOO_MANY_INDEXES_ERROR + indexes.size());
    }

    /**
     * Tests that the index hints serialization throws an exception when trying to serialize more indexes than can fit
     * in a short.
     */
    @Test
    public void testMaxHintsOnSerialization()
    {
        // prepare a set of indexes that exceeds the limit
        Set<IndexMetadata> indexes = new HashSet<>();
        for (int i = 0; i <= Short.MAX_VALUE; i++)
            indexes.add(IndexMetadata.fromSchemaMetadata("idx" + i, IndexMetadata.Kind.CUSTOM, Collections.emptyMap()));

        // test too many included indexes
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            IndexHints hints = IndexHints.create(indexes, null);
            Assertions.assertThatThrownBy(() -> IndexHints.serializer.serialize(hints, out, MessagingService.VERSION_51))
                      .isInstanceOf(AssertionError.class)
                      .hasMessageContaining(TOO_MANY_INDEXES_ERROR + indexes.size());
        }

        // test too many excluded indexes
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            IndexHints hints = IndexHints.create(null, indexes);
            Assertions.assertThatThrownBy(() -> IndexHints.serializer.serialize(hints, out, MessagingService.VERSION_51))
                      .isInstanceOf(AssertionError.class)
                      .hasMessageContaining(TOO_MANY_INDEXES_ERROR + indexes.size());
        }
    }

    @Test
    public void testMultipleIndexesPerColumnAndCaseSensitivity()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String sensitive = createIndex("CREATE INDEX sensitive ON %s(v) USING 'legacy_local_table'");
        String insensitive = createIndex("CREATE INDEX insensitive ON %s(v) USING 'sai' WITH OPTIONS = { 'case_sensitive' : false }");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, "A" };
        Object[] row2 = new Object[]{ 2, "a" };
        execute(insert, row1);
        execute(insert, row2);

        String query = "SELECT * FROM %s WHERE v='A'";
        assertThatIndexQueryPlanFor(query, row1, row2).selects(insensitive); // SAI has priority over legacy
        assertThatIndexQueryPlanFor(query + " WITH included_indexes={sensitive}", row1).selects(sensitive);
        assertThatIndexQueryPlanFor(query + " WITH included_indexes={insensitive}", row1, row2).selects(insensitive);
        assertThatIndexQueryPlanFor(query + " WITH excluded_indexes={sensitive}", row1, row2).selects(insensitive);
        assertThatIndexQueryPlanFor(query + " WITH excluded_indexes={insensitive}", row1).selects(sensitive);
        assertNeedsAllowFiltering(query + " WITH excluded_indexes={sensitive, insensitive}");
        assertThatIndexQueryPlanFor(query + " ALLOW FILTERING WITH excluded_indexes={sensitive, insensitive}", row1).selectsNone();
    }

    private void assertNonIncludableIndexesError(String query)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(IndexHints.NON_INCLUDABLE_INDEXES_ERROR);
    }

    private void assertNeedsAllowFiltering(String query)
    {
        assertNeedsAllowFiltering(() -> execute(query));
    }

    private void assertNeedsAllowFiltering(ThrowableAssert.ThrowingCallable callable)
    {
        Assertions.assertThatThrownBy(callable)
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(REQUIRES_ALLOW_FILTERING_MESSAGE);
    }

    private void assertIndexDoesNotSupportOperator(String query)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(REQUIRES_ALLOW_FILTERING_MESSAGE);
    }

    private void assertConflictingHints(String query, String indexName)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(CONFLICTING_INDEXES_ERROR + indexName);
    }

    private static Set<IndexMetadata> indexes(IndexMetadata... indexes)
    {
        Set<IndexMetadata> set = new HashSet<>(indexes.length);
        set.addAll(Arrays.asList(indexes));
        return set;
    }

    /**
     * Mock index with a common index group support.
     */
    public static final class GroupedIndex extends CustomIndexTest.IndexWithSharedGroup
    {
        private final ColumnMetadata indexedColumn;
        public static volatile IndexHints lastQueryIndexHints;

        public GroupedIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
            Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(baseCfs.metadata(), metadata);
            indexedColumn = target.left;
        }

        @Override
        public boolean supportsExpression(ColumnMetadata column, Operator operator)
        {
            return indexedColumn.name.equals(column.name);
        }

        @Override
        public Searcher searcherFor(ReadCommand command)
        {
            lastQueryIndexHints = command.rowFilter().indexHints;
            return super.searcherFor(command);
        }
    }
}
