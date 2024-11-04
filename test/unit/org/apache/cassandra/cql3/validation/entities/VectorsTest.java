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

package org.apache.cassandra.cql3.validation.entities;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.ServerTestUtils.daemonInitialization;

public class VectorsTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()     // overrides CQLTester.setUpClass()
    {
        daemonInitialization();
        prepareServer();
    }

    @Test
    public void testDescendingOrderingOfVectorIsSupported() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v vector<int, 3>, PRIMARY KEY (k, v)) WITH CLUSTERING ORDER BY (v DESC)");
        execute("INSERT INTO %s(k, v) VALUES (1, [1,2,3])");
        beforeAndAfterFlush(() -> assertRows(execute("SELECT v FROM %s WHERE k = 1 and v = [1,2,3]"), row(List.of(1, 2, 3))));
    }
}
