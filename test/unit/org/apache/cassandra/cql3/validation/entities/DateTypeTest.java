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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

public class DateTypeTest extends CQLTester
{
    /**
     * Check dates are correctly recognized and validated,
     * migrated from cql_tests.py:TestCQL.date_test()
     */
    @Test
    public void testDate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t timestamp)");

        execute("INSERT INTO %s (k, t) VALUES (0, '2011-02-03')");
        assertInvalid("INSERT INTO %s (k, t) VALUES (0, '2011-42-42')");
    }
}
