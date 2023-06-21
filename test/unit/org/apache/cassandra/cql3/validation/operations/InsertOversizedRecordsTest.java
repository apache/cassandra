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

import java.nio.ByteBuffer;

import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

public class InsertOversizedRecordsTest extends CQLTester
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
    }

    @Test
    public void compositeValuePk()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (a blob, b blob, PRIMARY KEY ((a, b)))");
        // sum of columns is too large
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, MEDIUM_BLOB))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Key length of " + (compositeElementCost(MEDIUM_BLOB.remaining()) * 2) + " is longer than maximum of 65535");

        // single column is too large
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, LARGE_BLOB))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Key length of " + LARGE_BLOB.remaining() + " is longer than maximum of 65535");
    }

    @Test
    public void singleValueClustering()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (a blob, b blob, PRIMARY KEY (a, b))");
        Assertions.assertThatThrownBy(() -> executeNet("INSERT INTO %s (a, b) VALUES (?, ?)", MEDIUM_BLOB, LARGE_BLOB))
                  .hasRootCauseInstanceOf(InvalidQueryException.class)
                  .hasRootCauseMessage("Key length of " + LARGE_BLOB.remaining() + " is longer than maximum of 65535");
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

    private static int compositeElementCost(int elementSize)
    {
        return elementSize + 2 + 1;
    }
}
