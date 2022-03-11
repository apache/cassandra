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

import org.apache.cassandra.db.ConsistencyLevel;

import static java.lang.String.format;
import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;

/**
 * Tests the guardrail for read consistency levels, {@link Guardrails#readConsistencyLevels}.
 */
public class GuardrailReadConsistencyLevelsTest extends GuardrailConsistencyLevelsTester
{
    public GuardrailReadConsistencyLevelsTest()
    {
        super("read_consistency_levels_warned",
              "read_consistency_levels_disallowed",
              Guardrails.readConsistencyLevels,
              Guardrails::getReadConsistencyLevelsWarned,
              Guardrails::getReadConsistencyLevelsDisallowed,
              Guardrails::getReadConsistencyLevelsWarnedCSV,
              Guardrails::getReadConsistencyLevelsDisallowedCSV,
              Guardrails::setReadConsistencyLevelsWarned,
              Guardrails::setReadConsistencyLevelsDisallowed,
              Guardrails::setReadConsistencyLevelsWarnedCSV,
              Guardrails::setReadConsistencyLevelsDisallowedCSV);
    }

    @Test
    public void testSelect() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v INT, PRIMARY KEY(k, c))");

        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 0, 2)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, 3)");

        testQuery("SELECT * FROM %s");
        testQuery("SELECT * FROM %s WHERE k = 0");
        testQuery("SELECT * FROM %s WHERE k = 0 AND c = 0");
    }

    private void testQuery(String query) throws Throwable
    {
        testQuery(query, ONE);
        testQuery(query, ALL);
        testQuery(query, QUORUM);
        testQuery(query, EACH_QUORUM);
        testQuery(query, LOCAL_ONE);
        testQuery(query, LOCAL_QUORUM);
    }

    private void testQuery(String query, ConsistencyLevel cl) throws Throwable
    {
        warnConsistencyLevels();
        disableConsistencyLevels();
        assertValid(query, cl);

        warnConsistencyLevels(cl);
        assertWarns(query, cl);

        disableConsistencyLevels(cl);
        assertFails(query, cl);
    }

    private void assertValid(String query, ConsistencyLevel cl) throws Throwable
    {
        assertValid(() -> execute(userClientState, query, cl));
    }

    private void assertWarns(String query, ConsistencyLevel cl) throws Throwable
    {
        assertWarns(() -> execute(userClientState, query, cl),
                    format("Provided values [%s] are not recommended for read consistency levels (warned values are: %s)",
                           cl, guardrails().getReadConsistencyLevelsWarned()));

        assertExcludedUsers(query, cl);
    }

    private void assertFails(String query, ConsistencyLevel cl) throws Throwable
    {
        assertFails(() -> execute(userClientState, query, cl),
                    format("Provided values [%s] are not allowed for read consistency levels (disallowed values are: %s)",
                           cl, guardrails().getReadConsistencyLevelsDisallowed()));

        assertExcludedUsers(query, cl);
    }

    private void assertExcludedUsers(String query, ConsistencyLevel cl) throws Throwable
    {
        assertValid(() -> execute(superClientState, query, cl));
        assertValid(() -> execute(systemClientState, query, cl));
    }
}
