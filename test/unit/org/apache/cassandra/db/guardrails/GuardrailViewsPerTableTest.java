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

import static java.lang.String.format;

/**
 * Tests the guardrail for the number of materialized views in a table, {@link Guardrails#materializedViewsPerTable}.
 */
public class GuardrailViewsPerTableTest extends ThresholdTester
{
    private static final int VIEWS_PER_TABLE_WARN_THRESHOLD = 1;
    private static final int VIEWS_PER_TABLE_FAIL_THRESHOLD = 3;

    private static final String CREATE_TABLE = "CREATE TABLE %s (k int PRIMARY KEY, v int)";
    private static final String CREATE_VIEW = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                                              "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)";

    public GuardrailViewsPerTableTest()
    {
        super(VIEWS_PER_TABLE_WARN_THRESHOLD,
              VIEWS_PER_TABLE_FAIL_THRESHOLD,
              Guardrails.materializedViewsPerTable,
              Guardrails::setMaterializedViewsPerTableThreshold,
              Guardrails::getMaterializedViewsPerTableWarnThreshold,
              Guardrails::getMaterializedViewsPerTableFailThreshold);
    }

    @Override
    protected long currentValue()
    {
        return getCurrentColumnFamilyStore().viewManager.size();
    }

    @Before
    public void before()
    {
        super.before();
        createTable(CREATE_TABLE);
    }

    @Test
    public void testCreateView() throws Throwable
    {
        String view1 = assertCreateViewSucceeds();
        assertCurrentValue(1);

        assertCreateViewWarns();
        assertCreateViewWarns();
        assertCreateViewFails();
        assertCurrentValue(3);

        // drop the first view, we should be able to create new MV again
        dropView(view1);
        assertCurrentValue(2);
        assertCreateViewWarns();
        assertCreateViewFails();
        assertCurrentValue(3);

        // previous guardrail should not apply to another base table
        createTable("CREATE TABLE %s (k int primary key, v int)");
        assertCreateViewSucceeds();
        assertCreateViewWarns();
        assertCreateViewWarns();
        assertCreateViewFails();
        assertCurrentValue(3);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");

        testExcludedUsers(() -> format("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                       "  WHERE k IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)",
                                       createViewName(), currentTable()),
                          () -> format("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                       "  WHERE k IS NOT NULL AND v2 IS NOT NULL PRIMARY KEY (v2, k)",
                                       createViewName(), currentTable()));
    }

    private String assertCreateViewSucceeds() throws Throwable
    {
        String viewName = createViewName();
        assertMaxThresholdValid(format(CREATE_VIEW, viewName));
        return viewName;
    }

    private void assertCreateViewWarns() throws Throwable
    {
        String viewName = createViewName();
        assertThresholdWarns(format(CREATE_VIEW, viewName),
                             format("Creating materialized view %s on table %s, current number of views %s exceeds warning threshold of %s.",
                                    viewName, currentTable(), currentValue() + 1, guardrails().getMaterializedViewsPerTableWarnThreshold()));
    }

    private void assertCreateViewFails() throws Throwable
    {
        String viewName = createViewName();
        assertThresholdFails(format(CREATE_VIEW, viewName),
                             format("aborting the creation of materialized view %s on table %s",
                                    viewName, currentTable()));
    }
}
