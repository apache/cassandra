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

import org.junit.Test;

import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;

public class GuardrailGroupByTest extends GuardrailTester
{
    private static final String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s' GROUP BY table_name",
                                                      SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                                      SchemaKeyspaceTables.TABLES,
                                                      KEYSPACE);

    private void setGuardrail(boolean enabled)
    {
        Guardrails.instance.setGroupByEnabled(enabled);
    }

    @Test
    public void checkExplicitlyDisabled() throws Throwable
    {
        setGuardrail(false);
        assertFails(query, "GROUP BY functionality is not allowed");
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        setGuardrail(false);
        testExcludedUsers(() -> query);
    }

    @Test
    public void checkEnabled() throws Throwable
    {
        setGuardrail(true);
        assertValid(query);
    }

    @Test
    public void checkView() throws Throwable
    {
        setGuardrail(false);
        createTable( "CREATE TABLE %s(pk int, ck int, v int, PRIMARY KEY(pk, ck))");
        String viewName = createView("CREATE MATERIALIZED VIEW %s AS " +
                                     "SELECT * FROM %s WHERE pk IS NOT null and ck IS NOT null " +
                                     "PRIMARY KEY(ck, pk)");
        String viewQuery = "SELECT * FROM " + viewName + " WHERE ck=0 GROUP BY pk";
        assertFails(viewQuery, "GROUP BY functionality is not allowed");
        testExcludedUsers(() -> viewQuery);
    }
}
