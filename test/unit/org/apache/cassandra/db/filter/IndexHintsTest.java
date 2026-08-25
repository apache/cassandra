/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.cql3.statements.SelectOptions;
import org.apache.cassandra.db.marshal.Redaction;
import org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
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
import static org.apache.cassandra.db.filter.IndexHints.CONFLICTING_INDEXES_ERROR;
import static org.apache.cassandra.db.filter.IndexHints.MISSING_INDEX_ERROR;
import static org.apache.cassandra.db.filter.IndexHints.TOO_MANY_INDEXES_ERROR;
import static org.apache.cassandra.db.filter.IndexHints.WILDCARD_INCLUDED_INDEXES_ERROR;
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
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_12);
        CQLTester.setUpClass();
        CQLTester.enableCoordinatorExecution();
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
        assertInvalidThrowMessage("Invalid value for property 'included_indexes'. It should be a set of identifiers.",
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
        Assertions.assertThat(command.toCQLString(Redaction.REDACT))
                  .doesNotContain("included_indexes")
                  .doesNotContain("excluded_indexes");

        // with empty hints
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 " +
                                     "WITH included_indexes={} AND excluded_indexes={}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString(Redaction.REDACT))
                  .doesNotContain("included_indexes")
                  .doesNotContain("excluded_indexes");

        // with included indexes only
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 " +
                                     "WITH included_indexes={idx1,idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString(Redaction.REDACT))
                  .contains(" WITH included_indexes = {idx1, idx2}")
                  .doesNotContain("excluded_indexes");

        // with excluded indexes only
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 ALLOW FILTERING " +
                                     "WITH excluded_indexes={idx1,idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString(Redaction.REDACT))
                  .contains(" WITH excluded_indexes = {idx1, idx2}")
                  .doesNotContain("included_indexes");

        // with both included and excluded indexes
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 ALLOW FILTERING " +
                                     "WITH included_indexes={idx1} AND excluded_indexes={idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString(Redaction.REDACT))
                  .contains(" WITH included_indexes = {idx1} AND excluded_indexes = {idx2}");

        // with a single-partition read command
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE k=1 AND a = 0 AND b = 0 ALLOW FILTERING " +
                                     "WITH included_indexes={idx1} AND excluded_indexes={idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString(Redaction.REDACT))
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

        // the wildcard is expanded to all the indexes on the table before it's sent over the wire, so the wire
        // format itself is unaffected by the wildcard
        testTransport(query + "WITH excluded_indexes={*}", IndexHints.create(indexes(), indexes(idx1, idx2, idx3, idx4)));
        testTransport(query + "WITH excluded_indexes={*, idx1}", IndexHints.create(indexes(), indexes(idx1, idx2, idx3, idx4)));
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
            ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_DS_12);
            Assertions.assertThat(ReadCommand.serializer.serializedSize(command, MessagingService.VERSION_DS_12))
                      .isEqualTo(out.buffer().remaining());
            DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
            command = ReadCommand.serializer.deserialize(in, MessagingService.VERSION_DS_12);
            actualHints = command.rowFilter().indexHints;
            Assertions.assertThat(actualHints).isEqualTo(expectedHints);

            // ...with a version that doesn't support index hints
            out = new DataOutputBuffer();
            if (expectedHints != IndexHints.NONE)
            {
                try
                {
                    ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_DS_11);
                }
                catch (IllegalStateException e)
                {
                    // expected
                    Assertions.assertThat(e)
                              .hasMessageContaining("Unable to serialize index hints with messaging version: " + MessagingService.VERSION_DS_11);
                }
            }
            else
            {
                ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_DS_11);
                Assertions.assertThat(ReadCommand.serializer.serializedSize(command, MessagingService.VERSION_DS_11))
                          .isEqualTo(out.buffer().remaining());
                in = new DataInputBuffer(out.buffer(), true);
                command = ReadCommand.serializer.deserialize(in, MessagingService.VERSION_DS_11);
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
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1)");
        String idx2 = createIndex("CREATE INDEX idx2 ON %s(v2)");

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
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1)");
        String idx2 = createIndex("CREATE INDEX idx2 ON %s(v2)");

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
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

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
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

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
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1)");
        String idx2 = createIndex("CREATE INDEX idx2 ON %s(v2)");

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
    public void testSASI()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'org.apache.cassandra.index.sasi.SASIIndex'");

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
        assertRejectsSASIQuery("SELECT * FROM %s WHERE v1=0", "v1");
        assertRejectsSASIQuery("SELECT * FROM %s WHERE v2=0", "v2");
        assertRejectsSASIQuery("SELECT * FROM %s WHERE v1=0 AND v2=0", "v1");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING", row1).selectsNone();

        // including idx1
        assertRejectsSASIQuery("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1}", "v1");
        assertRejectsSASIQuery("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx1}", "v2");
        assertRejectsSASIQuery("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1}", "v1");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1}");

        // including idx1 and idx2
        assertRejectsSASIQuery("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1,idx2}", "v1");
        assertRejectsSASIQuery("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx1,idx2}", "v2");
        assertRejectsSASIQuery("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1,idx2}", "v1");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}");

        // excluding idx1
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row1, row2).selectsNone();
        assertRejectsSASIQuery("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx1}", "v2");
        assertRejectsSASIQuery("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx1}", "v2");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row1).selectsNone();

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
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");
        String idx3 = createIndex("CREATE INDEX idx3 ON %s(v3)");

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
    public void testMultipleIndexesOnSameColumnLegacyAndSAI()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String sai = createIndex("CREATE CUSTOM INDEX sai ON %s(v) USING 'StorageAttachedIndex'");
        String legacy = createIndex("CREATE INDEX legacy ON %s(v)");

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
    public void testMultipleIndexesPerColumnAndUnsupportedEqOnAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String legacy = createIndex("CREATE INDEX legacy ON %s(v)");
        String literal = createIndex("CREATE CUSTOM INDEX literal ON %s(v) USING 'StorageAttachedIndex'");
        String analyzed = createIndex("CREATE CUSTOM INDEX analyzed ON %s(v) USING 'StorageAttachedIndex' " +
                                      "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'UNSUPPORTED', 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, "Richard Strauss" };
        Object[] row2 = new Object[]{ 2, "Johann Strauss" };
        Object[] row3 = new Object[]{ 3, "Strauss" };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // test index selection with EQ query
        String query = "SELECT * FROM %s WHERE v = 'Strauss' ";
        assertThatIndexQueryPlanFor(query, row3).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={legacy}", row3).selects(legacy).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={literal}", row3).selects(literal).doesntWarn();
        assertNonIncludableIndexesError(query + "WITH included_indexes={analyzed}"); // analyzed is not applicable to =
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal}"); // literal is selected over legacy
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,analyzed}"); // analyzed is not applicable to =
        assertNonIncludableIndexesError(query + "WITH included_indexes={literal,analyzed}"); // analyzed is not applicable to =
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal,analyzed}"); // analyzed is not applicable to =, literal is selected over legacy
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row3).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row3).selects(legacy).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={analyzed}", row3).selects(literal).doesntWarn();
        assertIndexDoesNotSupportOperator(query + "WITH excluded_indexes={legacy,literal}", "v");
        assertThatIndexQueryPlanFor(query + "ALLOW FILTERING WITH excluded_indexes={legacy,literal}", row3).selectsNone().doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,analyzed}", row3).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal,analyzed}", row3).selects(legacy).doesntWarn();
        assertNeedsAllowFiltering(query + "WITH excluded_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "ALLOW FILTERING WITH excluded_indexes={legacy,literal,analyzed}", row3).selectsNone();

        // test the same EQ query with a different value
        query = "SELECT * FROM %s WHERE v = 'Richard Strauss' ";
        assertThatIndexQueryPlanFor(query, row1).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={legacy}", row1).selects(legacy).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={literal}", row1).selects(literal).doesntWarn();
        assertNonIncludableIndexesError(query + "WITH included_indexes={analyzed}"); // analyzed is not applicable to =
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal}"); // literal is selected over legacy
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,analyzed}");
        assertNonIncludableIndexesError(query + "WITH included_indexes={literal,analyzed}");
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row1).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row1).selects(legacy).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={analyzed}", row1).selects(literal).doesntWarn();
        assertIndexDoesNotSupportOperator(query + "WITH excluded_indexes={legacy,literal}", "v");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,analyzed}", row1).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal,analyzed}", row1).selects(legacy).doesntWarn();
        assertNeedsAllowFiltering(query + "WITH excluded_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "ALLOW FILTERING WITH excluded_indexes={legacy,literal,analyzed}", row1).selectsNone().doesntWarn();

        // test index selection with MATCH query
        query = "SELECT * FROM %s WHERE v : 'strauss' ";
        assertThatIndexQueryPlanFor(query, row1, row2, row3).selects(analyzed).doesntWarn();
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy}"); // legacy is not applicable to :
        assertNonIncludableIndexesError(query + "WITH included_indexes={literal}"); // literal is not applicable to :
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1, row2, row3).selects(analyzed).doesntWarn();
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal}"); // legacy and literal are not applicable to :
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,analyzed}");
        assertNonIncludableIndexesError(query + "WITH included_indexes={literal,analyzed}");
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row1, row2, row3).selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row1, row2, row3).selects(analyzed).doesntWarn();
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={analyzed}", "v");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1, row2, row3).selects(analyzed).doesntWarn();
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={legacy,analyzed}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={literal,analyzed}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes={legacy,literal,analyzed}", "v", "strauss");

        // test the same MATCH query with a different value
        query = "SELECT * FROM %s WHERE v : 'Richard Strauss' ";
        assertThatIndexQueryPlanFor(query, row1).selects(analyzed).doesntWarn();
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy}"); // legacy is not applicable to :
        assertNonIncludableIndexesError(query + "WITH included_indexes={literal}"); // literal is not applicable to :
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1).selects(analyzed).doesntWarn();
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal}"); // legacy and literal are not applicable to :
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,analyzed}");
        assertNonIncludableIndexesError(query + "WITH included_indexes={literal,analyzed}");
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row1).selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row1).selects(analyzed).doesntWarn();
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={analyzed}", "v");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1).selects(analyzed).doesntWarn();
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={legacy,analyzed}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={literal,analyzed}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes={legacy,literal,analyzed}", "v", "Richard Strauss");
    }

    @Test
    public void testMultipleIndexesPerColumnAndMatchEqOnAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String legacy = createIndex("CREATE INDEX legacy ON %s(v)");
        String literal = createIndex("CREATE CUSTOM INDEX literal ON %s(v) USING 'StorageAttachedIndex'");
        String analyzed = createIndex("CREATE CUSTOM INDEX analyzed ON %s(v) USING 'StorageAttachedIndex' " +
                                      "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'MATCH', 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, "Richard Strauss" };
        Object[] row2 = new Object[]{ 2, "Johann Strauss" };
        Object[] row3 = new Object[]{ 3, "Strauss" };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // equality queries using the analyzed index should emit a warning
        String warning = format(AnalyzerEqOperatorSupport.EQ_RESTRICTION_ON_ANALYZED_WARNING, 'v', analyzed);

        // test index selection with EQ query, the hints will disambiguate the query and will produce different results
        // depending on the selected index
        String query = "SELECT * FROM %s WHERE v = 'Strauss' ";
        assertEqualityPredicateIsAmbiguous(query);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={legacy}", row3).selects(legacy).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={literal}", row3).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1, row2, row3).selects(analyzed).warns(warning);
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal}");
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={legacy,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={literal,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={legacy,literal,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes={legacy}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes={literal}");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={analyzed}", row3).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1, row2, row3).selects(analyzed).warns(warning);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,analyzed}", row3).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal,analyzed}", row3).selects(legacy).doesntWarn();
        assertNeedsAllowFiltering(query + "WITH excluded_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "ALLOW FILTERING WITH excluded_indexes={legacy,literal,analyzed}", row3).selectsNone().doesntWarn();

        // test the same EQ query with a different value
        query = "SELECT * FROM %s WHERE v = 'Richard Strauss' ";
        assertEqualityPredicateIsAmbiguous(query);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={legacy}", row1).selects(legacy).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={literal}", row1).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1).selects(analyzed).warns(warning);
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal}");
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={legacy,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={literal,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={legacy,literal,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes={legacy}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes={literal}");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={analyzed}", row1).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1).selects(analyzed).warns(warning);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,analyzed}", row1).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal,analyzed}", row1).selects(legacy).doesntWarn();
        assertNeedsAllowFiltering(query + "WITH excluded_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "ALLOW FILTERING WITH excluded_indexes={legacy,literal,analyzed}", row1).selectsNone().doesntWarn();

        // test index selection with MATCH query
        query = "SELECT * FROM %s WHERE v : 'strauss' ";
        assertThatIndexQueryPlanFor(query, row1, row2, row3).selects(analyzed).doesntWarn();
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy}"); // legacy is not applicable to :
        assertNonIncludableIndexesError(query + "WITH included_indexes={literal}"); // literal is not applicable to :
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1, row2, row3).selects(analyzed).doesntWarn();
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal}"); // legacy and literal are not applicable to :
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,analyzed}");
        assertNonIncludableIndexesError(query + "WITH included_indexes={literal,analyzed}");
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row1, row2, row3).selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row1, row2, row3).selects(analyzed).doesntWarn();
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={analyzed}", "v");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1, row2, row3).selects(analyzed).doesntWarn();
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={legacy,analyzed}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={literal,analyzed}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes={legacy,literal,analyzed}", "v", "strauss");

        // test the same MATCH query with a different value
        query = "SELECT * FROM %s WHERE v : 'Richard Strauss' ";
        assertThatIndexQueryPlanFor(query, row1).selects(analyzed);
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy}"); // legacy is not applicable to :
        assertNonIncludableIndexesError(query + "WITH included_indexes={literal}"); // literal is not applicable to :
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1).selects(analyzed).doesntWarn();
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal}"); // legacy and literal are not applicable to :
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,analyzed}");
        assertNonIncludableIndexesError(query + "WITH included_indexes={literal,analyzed}");
        assertNonIncludableIndexesError(query + "WITH included_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row1).selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row1).selects(analyzed).doesntWarn();
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={analyzed}", "v");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1).selects(analyzed).doesntWarn();
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={legacy,analyzed}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={literal,analyzed}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes={legacy,literal,analyzed}", "v", "Richard Strauss");
    }

    @Test
    public void testMultipleIndexesPerColumnAndContains()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<text>)");
        String legacy = createIndex("CREATE INDEX legacy ON %s(v)");
        String literal = createIndex("CREATE CUSTOM INDEX literal ON %s(v) USING 'StorageAttachedIndex'");
        String analyzed = createIndex("CREATE CUSTOM INDEX analyzed ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, list("Johann Strauss") };
        Object[] row2 = new Object[]{ 2, list("Richard Strauss", "Johann Sebastian Bach") };
        execute(insert, row1);
        execute(insert, row2);

        // CONTAINS and NOT CONTAINS with hints including the legacy index.
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH included_indexes={legacy}", row2).selects(legacy).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH included_indexes={legacy}").selects(legacy).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH included_indexes={legacy}").selects(legacy).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH included_indexes={legacy}").selects(legacy).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH included_indexes={legacy}").selects(legacy).doesntWarn();
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH included_indexes={legacy}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH included_indexes={legacy}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH included_indexes={legacy}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH included_indexes={legacy}");
        assertNonIncludableIndexesError("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH included_indexes={legacy}");

        // CONTAINS and NOT CONTAINS with hints including the non-analyzed index.
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH included_indexes={literal}", row2).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH included_indexes={literal}").selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH included_indexes={literal}").selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH included_indexes={literal}").selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH included_indexes={literal}").selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH included_indexes={literal}", row1).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH included_indexes={literal}", row1, row2).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH included_indexes={literal}", row1, row2).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH included_indexes={literal}", row1, row2).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH included_indexes={literal}", row1, row2).selects(literal).doesntWarn();

        // CONTAINS and NOT CONTAINS with hints including the analyzed index.
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH included_indexes={analyzed}", row2).selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH included_indexes={analyzed}", row1, row2).selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH included_indexes={analyzed}", row1, row2).selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH included_indexes={analyzed}", row2).selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH included_indexes={analyzed}").selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH included_indexes={analyzed}").selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH included_indexes={analyzed}").selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH included_indexes={analyzed}").selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH included_indexes={analyzed}", row1).selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH included_indexes={analyzed}", row1, row2).selects(analyzed).doesntWarn();

        // CONTAINS and NOT CONTAINS with hints excluding the legacy index.
        String warning = format(SingleColumnRestriction.ContainsRestriction.MULTIPLE_INDEXES_WARNING, 'v', literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH excluded_indexes={legacy}", row2).selects(literal).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH excluded_indexes={legacy}").selects(literal).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH excluded_indexes={legacy}").selects(literal).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH excluded_indexes={legacy}").selects(literal).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH excluded_indexes={legacy}").selects(literal).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH excluded_indexes={legacy}", row1).selects(literal).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH excluded_indexes={legacy}", row1, row2).selects(literal).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH excluded_indexes={legacy}", row1, row2).selects(literal).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH excluded_indexes={legacy}", row1, row2).selects(literal).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH excluded_indexes={legacy}", row1, row2).selects(literal).warns(warning);

        // CONTAINS and NOT CONTAINS with hints excluding the non-analyzed index.
        warning = format(SingleColumnRestriction.ContainsRestriction.MULTIPLE_INDEXES_WARNING, 'v', legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH excluded_indexes={literal}", row2).selects(legacy).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH excluded_indexes={literal}").selects(legacy).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH excluded_indexes={literal}").selects(legacy).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH excluded_indexes={literal}").selects(legacy).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH excluded_indexes={literal}").selects(legacy).warns(warning);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH excluded_indexes={literal}").selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH excluded_indexes={literal}").selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH excluded_indexes={literal}").selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH excluded_indexes={literal}", row1).selects(analyzed).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH excluded_indexes={literal}", row1, row2).selects(analyzed).doesntWarn();

        // CONTAINS and NOT CONTAINS with hints excluding the analyzed index.
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH excluded_indexes={analyzed}", row2).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH excluded_indexes={analyzed}").selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH excluded_indexes={analyzed}").selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH excluded_indexes={analyzed}").selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH excluded_indexes={analyzed}").selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH excluded_indexes={analyzed}", row1).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH excluded_indexes={analyzed}", row1, row2).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH excluded_indexes={analyzed}", row1, row2).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH excluded_indexes={analyzed}", row1, row2).selects(literal).doesntWarn();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH excluded_indexes={analyzed}", row1, row2).selects(literal).doesntWarn();
    }

    @Test
    public void testDuplicatedHints()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1)");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1,idx1}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx2,idx2}").selects(idx2);

        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx1,idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx2,idx2}");

        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx1}").selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx2}").selectsNone();

        assertConflictingHints("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1} AND excluded_indexes={idx1}", "idx1");
        assertConflictingHints("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx2} AND excluded_indexes={idx2}", "idx2");
    }

    /**
     * Tests the {@code *} wildcard in {@code excluded_indexes}, which should be equivalent to explicitly excluding
     * every index applicable to the table.
     */
    @Test
    public void testWildcardExcludedIndexes()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1)");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");
        Object[] row = row(0, 0, 0, 0);
        execute("INSERT INTO %s (k, v1, v2, v3) VALUES (0, 0, 0, 0)");

        // the wildcard excludes every applicable index, same as explicitly excluding all of them
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={*}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={*}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={*}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={*}", row).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={*}", row).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={*}", row).selectsNone();

        // an unrestricted column (v3) has no index anyway, so the wildcard exclusion doesn't change anything for it
        execute("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={*}");

        // the wildcard combined with explicit names in the same set is redundant, but not an error
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={*, idx1}", row).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={*, idx2}", row).selectsNone();

        // the wildcard is not allowed in included_indexes
        assertInvalidThrowMessage(IndexHints.WILDCARD_INCLUDED_INDEXES_ERROR,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={*}");

        // a wildcard exclusion conflicts with an included index that exists on the table
        assertConflictingHints("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1} AND excluded_indexes={*}", "idx1");
        assertConflictingHints("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx2} AND excluded_indexes={*}", "idx2");
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
            Assertions.assertThatThrownBy(() -> IndexHints.serializer.serialize(hints, out, MessagingService.VERSION_DS_12))
                      .isInstanceOf(AssertionError.class)
                      .hasMessageContaining(TOO_MANY_INDEXES_ERROR + indexes.size());
        }

        // test too many excluded indexes
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            IndexHints hints = IndexHints.create(null, indexes);
            Assertions.assertThatThrownBy(() -> IndexHints.serializer.serialize(hints, out, MessagingService.VERSION_DS_12))
                      .isInstanceOf(AssertionError.class)
                      .hasMessageContaining(TOO_MANY_INDEXES_ERROR + indexes.size());
        }
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
                  .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
    }

    private void assertIndexDoesNotSupportOperator(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, column));
    }

    private void assertIndexDoesNotSupportAnalyzerMatches(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_ANALYZER_MATCHES_MESSAGE, column));
    }

    private void assertMatchNeedsIndex(String query, String column, String value)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(format(StatementRestrictions.RESTRICTION_REQUIRES_INDEX_MESSAGE,
                                     ':',
                                     format("%s : '%s'", column, value)));
    }

    private void assertEqualityPredicateIsAmbiguous(String query)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining("equality predicate is ambiguous");
    }

    private void assertConflictingHints(String query, String indexName)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(CONFLICTING_INDEXES_ERROR + indexName);
    }

    private void assertRejectsSASIQuery(String query, String column)
    {
        String errorMessage = format(StatementRestrictions.HAS_UNSUPPORTED_SASI_INDEX_RESTRICTION_MESSAGE, column);
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(errorMessage);
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
