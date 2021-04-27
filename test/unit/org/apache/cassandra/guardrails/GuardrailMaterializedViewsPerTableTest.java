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

package org.apache.cassandra.guardrails;


import java.util.Collections;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.DatabaseDescriptor;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class GuardrailMaterializedViewsPerTableTest extends GuardrailTester
{
    private static final String CREATE_TABLE = "CREATE TABLE %s (k int primary key, v int)";
    private static final String CREATE_VIEW = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                                              "WHERE k is NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)";

    private Long defaultMVPerTableFailureThreshold;

    @Before
    public void before()
    {
        defaultMVPerTableFailureThreshold = config().materialized_view_per_table_failure_threshold;
        config().materialized_view_per_table_failure_threshold = 1L;

        createTable(CREATE_TABLE);
    }

    @After
    public void after()
    {
        config().materialized_view_per_table_failure_threshold = defaultMVPerTableFailureThreshold;
    }

    @Test
    public void testCreateView() throws Throwable
    {
        String view1 = assertCreateViewSucceeds();
        assertNumViews(1);

        assertCreateViewFails();
        assertNumViews(1);

        // drop the first view, we should be able to create new MV again
        dropView(view1);
        assertNumViews(0);

        assertCreateViewSucceeds();
        assertNumViews(1);

        // previous guardrail should not apply to another base table
        createTable("CREATE TABLE %s (k int primary key, v int)");
        assertNumViews(0);

        assertCreateViewSucceeds();
        assertNumViews(1);

        assertCreateViewFails();
        assertNumViews(1);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        testExcludedUsers("CREATE MATERIALIZED VIEW excluded_1 AS SELECT * FROM %s " +
                          "  WHERE k is NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)",
                          "CREATE MATERIALIZED VIEW excluded_2 AS SELECT * FROM %s " +
                          "  WHERE k is NOT NULL AND v2 IS NOT NULL PRIMARY KEY (v2, k)",
                          "DROP MATERIALIZED VIEW excluded_1",
                          "DROP MATERIALIZED VIEW excluded_2");
    }

    private String assertCreateViewSucceeds() throws Throwable
    {
        String viewName = createViewName();
        assertValid(format(CREATE_VIEW, viewName));
        return viewName;
    }

    private void assertNumViews(int count)
    {
        assertEquals(count, getCurrentColumnFamilyStore().viewManager.size());
    }

    private void assertCreateViewFails() throws Throwable
    {
        String viewName = createViewName();
        String expectedMessage = String.format("failed to create materialized view %s on table %s",
                                               viewName, currentTable());
        assertFails(expectedMessage, format(CREATE_VIEW, viewName));
    }
}