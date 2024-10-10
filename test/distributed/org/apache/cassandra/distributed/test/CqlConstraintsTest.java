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

package org.apache.cassandra.distributed.test;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.cql3.ConstraintInvalidException;
import org.apache.cassandra.cql3.ConstraintViolationException;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;

public class CqlConstraintsTest extends TestBaseImpl
{
    @Test
    public void testInvalidConstraintsExceptions() throws IOException
    {
        final String tableName = KEYSPACE + ".tbl1";

        try (Cluster cluster = init(Cluster.build(3).start()))
        {
            assertThrowsInvalidConstraintException(cluster, String.format("CREATE TABLE %s (pk int, ck1 text, ck2 int, v int, " +
                                                                          "PRIMARY KEY ((pk), ck1, ck2), CONSTRAINT cons1 CHECK ck1 < 100);", tableName),
                                                   "ck1 is not a number");

            assertThrowsInvalidConstraintException(cluster, String.format("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, " +
                                                                          "PRIMARY KEY ((pk), ck1, ck2), CONSTRAINT cons1 CHECK LENGTH(ck1) < 100);", tableName),
                                                   "Column should be of type class org.apache.cassandra.db.marshal.UTF8Type or " +
                                                   "class org.apache.cassandra.db.marshal.AsciiType but got class org.apache.cassandra.db.marshal.Int32Type");
        }
    }

    @Test
    public void testScalarTableLevelConstraint() throws IOException
    {
        final String tableName = KEYSPACE + ".tbl1";

        try (Cluster cluster = init(Cluster.build(3).start()))
        {
            String createTableStatement = "CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk), ck1, ck2), CONSTRAINT cons1 CHECK ck1 < 100);";
            cluster.schemaChange(String.format(createTableStatement, tableName));
            cluster.coordinator(1).execute(String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 2, 2, 3)", tableName), ConsistencyLevel.ALL);

            assertThrowsConstraintViolationException(cluster,
                                                     String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 200, 2, 3)", tableName),
                                                     "ck1 value length should be smaller than 100");

            assertThrowsConstraintViolationException(cluster,
                                                     String.format("INSERT INTO %s (pk, ck1, ck2, v) VALUES (1, 100, 2, 3)", tableName),
                                                     "ck1 value length should be smaller than 100");
        }
    }

    private void assertThrowsConstraintViolationException(Cluster cluster, String statement, String description)
    {
        Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(statement, ConsistencyLevel.ALL))
                  .describedAs(description)
                  .has(new Condition<Throwable>(t -> t.getClass().getCanonicalName()
                                                      .equals(ConstraintViolationException.class.getCanonicalName()), "is instance of ConstraintViolationException"));
    }

    private void assertThrowsInvalidConstraintException(Cluster cluster, String statement, String description)
    {
        Assertions.assertThatThrownBy(() -> cluster.schemaChange(statement))
                  .describedAs(description)
                  .has(new Condition<Throwable>(t -> t.getClass().getCanonicalName()
                                                      .equals(ConstraintInvalidException.class.getCanonicalName()), "is instance of ConstraintInvalidException"));
    }
}
