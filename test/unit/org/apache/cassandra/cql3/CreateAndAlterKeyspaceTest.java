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
package org.apache.cassandra.cql3;

import org.junit.Test;

public class CreateAndAlterKeyspaceTest extends CQLTester
{
    @Test
    // tests CASSANDRA-9565
    public void testCreateAndAlterWithDoubleWith() throws Throwable
    {
        String[] stmts = new String[] {"ALTER KEYSPACE WITH WITH DURABLE_WRITES = true",
                                       "ALTER KEYSPACE ks WITH WITH DURABLE_WRITES = true",
                                       "CREATE KEYSPACE WITH WITH DURABLE_WRITES = true",
                                       "CREATE KEYSPACE ks WITH WITH DURABLE_WRITES = true"};

        for (String stmt : stmts) {
            assertInvalidSyntaxMessage("no viable alternative at input 'WITH'", stmt);
        }
    }
}
