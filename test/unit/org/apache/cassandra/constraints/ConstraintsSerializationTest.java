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

package org.apache.cassandra.constraints;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.constraints.ColumnConstraint;
import org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType;
import org.apache.cassandra.cql3.constraints.ColumnConstraints;
import org.apache.cassandra.cql3.constraints.ConstraintResolver;
import org.apache.cassandra.cql3.constraints.ConstraintViolationException;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Constraints are serialized with the schema in cluster metadata, e.g. in a snapshot a node sends to another one which
 * catches up. Their serialized size has to match what is written, and they have to be read back as they were.
 */
public class ConstraintsSerializationTest extends CQLTester
{
    // a constraint of each kind and of each built-in function, each also on a column whose name has to be quoted;
    // a new kind or function has to be added here, see everyConstraint()
    private static final String TABLE = "CREATE TABLE %s (pk int PRIMARY KEY, " +
                                        "s int CHECK s > 0 AND s < 100, " +
                                        "\"Quoted\" int CHECK \"Quoted\" < 10, " +
                                        "l text CHECK LENGTH() < 10, " +
                                        "\"Length\" text CHECK LENGTH() < 10, " +
                                        "o blob CHECK OCTET_LENGTH() < 10, " +
                                        "\"Octet\" blob CHECK OCTET_LENGTH() < 10, " +
                                        "r text CHECK REGEXP() = 'a.*b', " +
                                        "\"Regexp\" text CHECK REGEXP() = 'a.*b', " +
                                        "n int CHECK NOT NULL, " +
                                        "\"NotNull\" int CHECK NOT NULL, " +
                                        "j text CHECK JSON(), " +
                                        "\"Json\" text CHECK JSON(), " +
                                        "\"Mixed\" text CHECK NOT NULL AND LENGTH() < 10)";

    /**
     * Fails when a kind of constraint or a built-in constraint function is added, but not to {@link #TABLE}, so its
     * serialization is not tested.
     */
    @Test
    public void everyConstraint() throws Throwable
    {
        createTable(TABLE);
        Set<ConstraintType> types = EnumSet.noneOf(ConstraintType.class);
        Set<String> functions = new HashSet<>();
        for (ColumnMetadata column : currentTableMetadata().columns())
        {
            if (column.hasConstraint())
                collect(column.getColumnConstraints(), types, functions);
        }

        assertThat(types).as("kinds of constraints in the table").containsExactlyInAnyOrder(ConstraintType.values());
        Set<String> builtIn = Stream.concat(Arrays.stream(ConstraintResolver.Functions.values()),
                                            Arrays.stream(ConstraintResolver.UnaryFunctions.values()))
                                    .map(Enum::name)
                                    .collect(Collectors.toSet());
        assertThat(functions).as("constraint functions in the table").containsExactlyInAnyOrderElementsOf(builtIn);
    }

    private static void collect(ColumnConstraint<?> constraint, Set<ConstraintType> types, Set<String> functions)
    {
        ConstraintType type = constraint.getConstraintType();
        types.add(type);
        if (type == ConstraintType.COMPOSED)
        {
            for (ColumnConstraint<?> composed : ((ColumnConstraints) constraint).getConstraints())
                collect(composed, types, functions);
        }
        else if (type == ConstraintType.FUNCTION || type == ConstraintType.UNARY_FUNCTION)
        {
            functions.add(constraint.name());
        }
    }

    @Test
    public void column() throws Throwable
    {
        createTable(TABLE);
        TableMetadata table = currentTableMetadata();
        List<String> problems = new ArrayList<>();
        for (ColumnMetadata column : table.columns())
        {
            if (!column.hasConstraint())
                continue;
            for (Version version : Version.values())
            {
                if (version == Version.UNKNOWN || !version.isAtLeast(Version.V6))
                    continue;
                DataOutputBuffer out = new DataOutputBuffer();
                ColumnMetadata.serializer.serialize(column, out, version);
                long size = ColumnMetadata.serializer.serializedSize(column, version);
                if (size != out.getLength())
                    problems.add(format("%s %s: serializedSize %d, written %d", column.name, version, size, out.getLength()));
                ColumnMetadata read = ColumnMetadata.serializer.deserialize(new DataInputBuffer(out.getData()), Types.none(), UserFunctions.none(), version);
                if (!column.equals(read))
                    problems.add(format("%s %s: read %s, expected %s", column.name, version, read.getColumnConstraints(), column.getColumnConstraints()));
            }
        }
        assertThat(problems).isEmpty();
    }

    @Test
    public void clusterMetadata() throws Throwable
    {
        createTable(TABLE);
        ClusterMetadata metadata = ClusterMetadata.current();
        Version version = NodeVersion.CURRENT_METADATA_VERSION;
        DataOutputBuffer out = new DataOutputBuffer();
        ClusterMetadata.serializer.serialize(metadata, out, version);
        assertThat(ClusterMetadata.serializer.serializedSize(metadata, version)).as("serialized size").isEqualTo(out.getLength());

        ClusterMetadata read = ClusterMetadata.serializer.deserialize(new DataInputBuffer(out.getData()), version);
        assertThat(read.schema.getKeyspaceMetadata(KEYSPACE).getTableOrViewNullable(currentTable()))
        .isEqualTo(metadata.schema.getKeyspaceMetadata(KEYSPACE).getTableOrViewNullable(currentTable()));
    }

    /**
     * A table is created from its CQL rendered again, which has to quote the column of a scalar constraint.
     */
    @Test
    public void quotedColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, \"Quoted\" int CHECK \"Quoted\" < 10, \"Other\" int)");
        execute("INSERT INTO %s (pk, \"Quoted\") VALUES (1, 5)");
        assertInvalidThrow(ConstraintViolationException.class, "INSERT INTO %s (pk, \"Quoted\") VALUES (2, 50)");

        execute("ALTER TABLE %s ALTER \"Other\" CHECK \"Other\" > 0");
        execute("INSERT INTO %s (pk, \"Other\") VALUES (3, 1)");
        assertInvalidThrow(ConstraintViolationException.class, "INSERT INTO %s (pk, \"Other\") VALUES (4, -1)");

        assertThat(currentTableMetadata().toCqlString(false, false, false)).contains("\"Quoted\" int CHECK \"Quoted\" < 10",
                                                                                    "\"Other\" int CHECK \"Other\" > 0");
        assertRows(execute("SELECT pk, \"Quoted\", \"Other\" FROM %s WHERE pk IN (1, 3)"),
                   row(1, 5, null),
                   row(3, null, 1));
    }

    /**
     * Function constraints do not name their column in CQL, but they are created from CQL rendered again too.
     */
    @Test
    public void quotedColumnFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, " +
                    "\"Length\" text CHECK LENGTH() < 10, " +
                    "\"Octet\" blob CHECK OCTET_LENGTH() < 10, " +
                    "\"Regexp\" text CHECK REGEXP() = 'a.*b', " +
                    "\"NotNull\" int CHECK NOT NULL, " +
                    "\"Json\" text CHECK JSON(), " +
                    "\"Other\" text)");
        execute("INSERT INTO %s (pk, \"Length\", \"Octet\", \"Regexp\", \"NotNull\", \"Json\") VALUES (1, 'short', 0x01, 'axb', 1, '{}')");
        assertInvalidThrow(ConstraintViolationException.class, "INSERT INTO %s (pk, \"NotNull\", \"Length\") VALUES (2, 1, 'far too long')");
        assertInvalidThrow(ConstraintViolationException.class, "INSERT INTO %s (pk, \"NotNull\", \"Octet\") VALUES (2, 1, 0x0102030405060708090a0b)");
        assertInvalidThrow(ConstraintViolationException.class, "INSERT INTO %s (pk, \"NotNull\", \"Regexp\") VALUES (2, 1, 'xyz')");
        assertInvalidThrow(ConstraintViolationException.class, "INSERT INTO %s (pk, \"NotNull\") VALUES (2, null)");
        assertInvalidThrow(ConstraintViolationException.class, "INSERT INTO %s (pk, \"NotNull\", \"Json\") VALUES (2, 1, 'not json')");

        execute("ALTER TABLE %s ALTER \"Other\" CHECK NOT NULL AND LENGTH() < 10");
        execute("INSERT INTO %s (pk, \"NotNull\", \"Other\") VALUES (3, 1, 'short')");
        assertInvalidThrow(ConstraintViolationException.class, "INSERT INTO %s (pk, \"NotNull\", \"Other\") VALUES (4, 1, 'far too long')");

        assertThat(currentTableMetadata().toCqlString(false, false, false)).contains("\"Length\" text CHECK LENGTH() < 10",
                                                                                    "\"Octet\" blob CHECK OCTET_LENGTH() < 10",
                                                                                    "\"Regexp\" text CHECK REGEXP() = 'a.*b'",
                                                                                    "\"NotNull\" int CHECK NOT NULL",
                                                                                    "\"Json\" text CHECK JSON",
                                                                                    "\"Other\" text CHECK NOT NULL AND LENGTH() < 10");
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s"), row(1), row(3));
    }

    /**
     * A scalar constraint read back from cluster metadata, as on a restart, has to enforce its value.
     */
    @Test
    public void scalarConstraintEnforcedAfterDeserialization() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, b int CHECK b > 0)");
        Version version = NodeVersion.CURRENT_METADATA_VERSION;
        DataOutputBuffer out = new DataOutputBuffer();
        ClusterMetadata.serializer.serialize(ClusterMetadata.current(), out, version);
        ClusterMetadata read = ClusterMetadata.serializer.deserialize(new DataInputBuffer(out.getData()), version);

        TableMetadata table = read.schema.getKeyspaceMetadata(KEYSPACE).getTableOrViewNullable(currentTable());
        ColumnConstraints constraints = table.getColumn(ByteBufferUtil.bytes("b")).getColumnConstraints();

        constraints.evaluate(Int32Type.instance, Int32Type.instance.decompose(1));
        assertThatThrownBy(() -> constraints.evaluate(Int32Type.instance, Int32Type.instance.decompose(-1)))
        .isInstanceOf(ConstraintViolationException.class);
    }
}
