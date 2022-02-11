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

package org.apache.cassandra.db.guardrails;

import org.junit.Before;
import org.junit.Test;

public class GuardrailPartitionKeysInSelectTest extends ThresholdTester
{
    private static final int PARTITION_KEYS_SELECT_WARN_THRESHOLD = 3;
    private static final int PARTITION_KEYS_SELECT_ABORT_THRESHOLD = 5;

    public GuardrailPartitionKeysInSelectTest()
    {
        super(PARTITION_KEYS_SELECT_WARN_THRESHOLD,
              PARTITION_KEYS_SELECT_ABORT_THRESHOLD,
              "partition_keys_in_select",
              Guardrails::setPartitionKeysInSelectThreshold,
              Guardrails::getPartitionKeysInSelectWarnThreshold,
              Guardrails::getPartitionKeysInSelectFailThreshold);
    }

    @Before
    public void setupTest()
    {
        createKeyspace("CREATE KEYSPACE IF NOT EXISTS partition_keys_in_select_test_ks " +
                       "with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } " +
                       "and durable_writes = false");
        createTable("CREATE TABLE IF NOT EXISTS partition_keys_in_select_test_ks.partition_keys_in_select_test " +
                    "(k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
    }

    @Test
    public void testSelectStatementAgainstInClausePartitionKeys() throws Throwable
    {
        assertValid("SELECT k, c, v FROM partition_keys_in_select_test_ks.partition_keys_in_select_test " +
                    "WHERE k=10");

        assertValid("SELECT k, c, v FROM partition_keys_in_select_test_ks.partition_keys_in_select_test " +
                    "WHERE k IN (2, 3)");

        assertValid("SELECT k, c, v FROM partition_keys_in_select_test_ks.partition_keys_in_select_test " +
                    "WHERE k = 2 and c IN (2, 3, 4, 5, 6, 7)");

        assertWarns("Query with partition keys in IN clause on table partition_keys_in_select_test, " +
                    "with number of partition keys 4 exceeds warning threshold of 3.",
                    "SELECT k, c, v FROM partition_keys_in_select_test_ks.partition_keys_in_select_test " +
                        "WHERE k IN (2, 3, 4, 5)");

        assertFails("Aborting query with partition keys in IN clause on table partition_keys_in_select_test, " +
                     "number of partition keys 6 exceeds abort threshold of 5." ,
                     "SELECT k, c, v FROM partition_keys_in_select_test_ks.partition_keys_in_select_test " +
                     "WHERE k IN (2, 3, 4, 5, 6, 7)"
                     );
    }

    protected long currentValue()
    {
        throw new UnsupportedOperationException();
    }
}
