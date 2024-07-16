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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GuardrailAlterTableCompactionStrategyTest extends GuardrailTester
{
    private static final String querySameCS = "ALTER TABLE %s.%s WITH compaction = {'class': 'SizeTieredCompactionStrategy'};";
    private static final String queryDifferentCS = "ALTER TABLE %s.%s WITH compaction = {'class': 'LeveledCompactionStrategy'};";
    private void setGuardrail(boolean enabled)
    {
        Guardrails.instance.setAlterTableCompactionStrategyEnabled(enabled);
    }

    @Before
    public void beforeGuardrailTest() throws Throwable
    {
        super.beforeGuardrailTest();
        createTable("CREATE TABLE %s (key text, val int, primary key(key)) WITH compaction = {'class': 'SizeTieredCompactionStrategy'};");
    }

    @After
    public void after()
    {
        setGuardrail(true);
        // immediately drop the created table so its async cleanup doesn't interfere with the next tests
        if (currentTable() != null)
            dropTable("DROP TABLE %s");
    }

    @Test
    public void featureDisabled() throws Throwable
    {
        setGuardrail(true);
        assertValid(String.format(querySameCS, keyspace(), currentTable()));
        assertValid(String.format(queryDifferentCS, keyspace(), currentTable()));
    }

    @Test
    public void featureEnabled() throws Throwable
    {
        setGuardrail(false);
        assertFails(String.format(queryDifferentCS, keyspace(), currentTable()), "ALTER TABLE compaction strategy is not allowed");
        // query with the same CS should pass
        assertValid(String.format(querySameCS, keyspace(), currentTable()));
    }
}
