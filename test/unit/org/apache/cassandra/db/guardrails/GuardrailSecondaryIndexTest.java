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
 * Tests the guardrail for disabling user creation of secondary indexes, {@link Guardrails#setSecondaryIndexesEnabled(boolean)}.
 */
public class GuardrailSecondaryIndexTest extends GuardrailTester
{
    public GuardrailSecondaryIndexTest()
    {
        super(Guardrails.createSecondaryIndexesEnabled);
    }

    @Before
    public void setupTest()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)");
    }

    private void setGuardrail(boolean enabled)
    {
        guardrails().setSecondaryIndexesEnabled(enabled);
    }

    @Test
    public void testCreateIndex() throws Throwable
    {
        setGuardrail(true);
        assertValid(String.format("CREATE INDEX %s ON %s.%s(%s)", "v1_idx", keyspace(), currentTable(), "v1"));
        assertValid(String.format("CREATE INDEX %s ON %s.%s(%s)", "v2_idx", keyspace(), currentTable(), "v2"));

        setGuardrail(false);
        assertFails(String.format("CREATE INDEX %s ON %s.%s(%s)", "v3_idx", keyspace(), currentTable(), "v3"), "Creating secondary indexes");
        assertFails(String.format("CREATE INDEX %s ON %s.%s(%s)", "v4_idx", keyspace(), currentTable(), "v4"), "Creating secondary indexes");
        assertFails(String.format("CREATE INDEX %s ON %s.%s(%s)", "v2_idx", keyspace(), currentTable(), "v2"), "Creating secondary indexes");

        setGuardrail(true);
        assertValid(String.format("CREATE INDEX %s ON %s.%s(%s)", "v3_idx", keyspace(), currentTable(), "v3"));
        assertValid(String.format("CREATE INDEX %s ON %s.%s(%s)", "v4_idx", keyspace(), currentTable(), "v4"));

        // Confirm can drop in either state
        setGuardrail(false);
        dropIndex(format("DROP INDEX %s.%s", keyspace(), "v1_idx"));

        setGuardrail(true);
        dropIndex(format("DROP INDEX %s.%s", keyspace(), "v2_idx"));
    }

    @Test
    public void testCustomIndex() throws Throwable
    {
        // 2i guardrail will also affect custom indexes
        setGuardrail(false);
        assertFails(format("CREATE CUSTOM INDEX ON %%s (%s) USING 'org.apache.cassandra.index.sasi.SASIIndex'", "v4"),
                    format("Creating secondary indexes", currentTable())
        );

        // Confirm custom creation will work on flip
        setGuardrail(true);
        assertValid(format("CREATE CUSTOM INDEX ON %%s (%s) USING 'org.apache.cassandra.index.sasi.SASIIndex'", "v4"));
    }
}
