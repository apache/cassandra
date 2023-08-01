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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

import com.google.common.base.StandardSystemProperty;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;

public class InsertInvalidateSizedRecordsTest extends CQLTester
{
    private static final ByteBuffer LARGE_BLOB = ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT + 1);
    private static final ByteBuffer MEDIUM_BLOB = ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT / 2 + 10);

    {
        requireNetwork();
    }

    @Test
    public void singleValuePk()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (a blob PRIMARY KEY)");
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a) VALUES (?)", LARGE_BLOB))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Key length of " + LARGE_BLOB.remaining() + " is longer than maximum of 65535");

        // null / empty checks
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a) VALUES (?)", new Object[] {null}))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Invalid null value in condition for column a");
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a) VALUES (?)", EMPTY_BYTE_BUFFER))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Key may not be empty");
    }

    @Test
    public void compositeValuePk()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (a blob, b blob, PRIMARY KEY ((a, b)))");
        // sum of columns is too large
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, MEDIUM_BLOB))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Key length of " + (MEDIUM_BLOB.remaining() * 2) + " is longer than maximum of 65535");

        // single column is too large
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, LARGE_BLOB))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Key length of " + (MEDIUM_BLOB.remaining() + LARGE_BLOB.remaining()) + " is longer than maximum of 65535");

        // null / empty checks
        // this is an inconsistent behavior... null is blocked by org.apache.cassandra.db.MultiCBuilder.OneClusteringBuilder.addElementToAll
        // but this does not count empty as null, and doesn't check for this case...  We have a requirement in cqlsh that empty is allowed when
        // user opts-in to allow it (NULL='-'), so we will find that null is blocked, but empty is allowed!
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", new Object[] {null, null}))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Invalid null value in condition for column a");
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", new Object[] {MEDIUM_BLOB, null}))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Invalid null value in condition for column b");
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", new Object[] {null, MEDIUM_BLOB}))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Invalid null value in condition for column a");

        // empty is allowed when composite partition columns...
        executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", EMPTY_BYTE_BUFFER, EMPTY_BYTE_BUFFER);
        execute("TRUNCATE %s");

        executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, EMPTY_BYTE_BUFFER);
        execute("TRUNCATE %s");

        executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", EMPTY_BYTE_BUFFER, MEDIUM_BLOB);
    }

    @Test
    public void singleValueClustering()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (a blob, b blob, PRIMARY KEY (a, b))");
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, LARGE_BLOB))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Key length of " + LARGE_BLOB.remaining() + " is longer than maximum of 65535");

        // null / empty checks
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, null))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Invalid null value in condition for column b");

        // org.apache.cassandra.db.MultiCBuilder.OneClusteringBuilder.addElementToAll defines "null" differently than most of the code;
        // most of the code defines null as:
        //   value == null || accessor.isEmpty(value)
        // but the code defines null as
        //   value == null
        // In CASSANDRA-18504 a new isNull method was added to the type, as blob and text both "should" allow empty, but this scattered null logic doesn't allow...
        // For backwards compatability reasons, need to keep empty support
        executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, EMPTY_BYTE_BUFFER);
    }

    @Test
    public void compositeValueClustering()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (a blob, b blob, c blob, PRIMARY KEY (a, b, c))");
        // sum of columns is too large
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", MEDIUM_BLOB, MEDIUM_BLOB, MEDIUM_BLOB))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Key length of " + (MEDIUM_BLOB.remaining() * 2) + " is longer than maximum of 65535");

        // single column is too large
        // the logic prints the total clustering size and not the single column's size that was too large
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", MEDIUM_BLOB, MEDIUM_BLOB, LARGE_BLOB))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Key length of " + (MEDIUM_BLOB.remaining() + LARGE_BLOB.remaining()) + " is longer than maximum of 65535");
    }

    @Test
    public void singleValueIndex()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (a blob, b blob, PRIMARY KEY (a))");
        String table = KEYSPACE + "." + currentTable();
        execute("CREATE INDEX single_value_index ON %s (b)");
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, LARGE_BLOB))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Cannot index value of size " + LARGE_BLOB.remaining() + " for index single_value_index on " + table + "(b) (maximum allowed size=65535)");
    }

    /**
     * Partial port of cqlsh_tests.test_cqlsh_copy.TestCqlshCopy#test_reading_empty_strings_for_different_types
     */
    @Test
    public void readingEmptyStringsForDifferentTypes() throws IOException
    {
        createTable(KEYSPACE, "CREATE TABLE %s (\n" +
                              "                a text,\n" +
                              "                b text,\n" +
                              "                c text,\n" +
                              "                d text,\n" +
                              "                o uuid,\n" +
                              "                 i1 bigint,\n" +
                              "                 i2 bigint,\n" +
                              "                 t text,\n" +
                              "                 i3 bigint,\n" +
                              "                 PRIMARY KEY ((a, b, c, d), o)\n" +
                              "            )");

        File csv = new File(Files.createTempFile(new File(StandardSystemProperty.JAVA_IO_TMPDIR.value()).toPath(), "test_reading_empty_strings_for_different_types", ".csv"));
        try (Writer out = new OutputStreamWriter(new BufferedOutputStream(csv.newOutputStream(File.WriteMode.OVERWRITE)), StandardCharsets.UTF_8))
        {
            out.write(",,,a1,645e7d3c-aef7-4e3c-b834-24b792cf2e55,,,,r1\n");
        }
        String table = KEYSPACE + "." + currentTable();
        String template = "COPY %s FROM '%s' WITH NULL='-' AND PREPAREDSTATEMENTS = %s";
        // This is different from cqlsh_tests.test_cqlsh_copy.TestCqlshCopy#test_reading_empty_strings_for_different_types as "false" actually is broken!
        // If you do false, then the parsing actually fails... and the test didn't actually check the return code from cqlsh so doesn't detect that it failed!
        // What is worse is the test didn't truncate either, which means that the "false" case is reading the "true" case data!
        // Rather than try to fix the test and cqlsh... this test only keeps the "true" logic as it is what is needed to make sure CASSANDRA-18504
        // didn't break things.
        ToolRunner.invokeCqlsh(String.format(template, table, csv.absolutePath(), true)).assertOnCleanExit();
        assertRowsNet(executeNet("SELECT * FROM %s"), row("", "", "", "a1", UUID.fromString("645e7d3c-aef7-4e3c-b834-24b792cf2e55"), null, null, null, "r1"));
    }
}
