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

import org.apache.cassandra.db.ConsistencyLevel;

import static java.lang.String.format;
import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.db.ConsistencyLevel.ANY;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;

/**
 * Tests the guardrail for write consistency levels, {@link Guardrails#writeConsistencyLevels}.
 */
public class GuardrailWriteConsistencyLevelsTest extends GuardrailConsistencyLevelsTester
{
    public GuardrailWriteConsistencyLevelsTest()
    {
        super("write_consistency_levels_warned",
              "write_consistency_levels_disallowed",
              Guardrails.writeConsistencyLevels,
              Guardrails::getWriteConsistencyLevelsWarned,
              Guardrails::getWriteConsistencyLevelsDisallowed,
              Guardrails::getWriteConsistencyLevelsWarnedCSV,
              Guardrails::getWriteConsistencyLevelsDisallowedCSV,
              Guardrails::setWriteConsistencyLevelsWarned,
              Guardrails::setWriteConsistencyLevelsDisallowed,
              Guardrails::setWriteConsistencyLevelsWarnedCSV,
              Guardrails::setWriteConsistencyLevelsDisallowedCSV);
    }

    @Before
    public void before()
    {
        super.before();
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
    }

    @Test
    public void testInsert() throws Throwable
    {
        testQuery("INSERT INTO %s (k, c, v) VALUES (1, 2, 'val')");
        testLWTQuery("INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') IF NOT EXISTS");
    }

    @Test
    public void testUpdate() throws Throwable
    {
        testQuery("UPDATE %s SET v = 'val2' WHERE k = 1 AND c = 2");
        testLWTQuery("UPDATE %s SET v = 'val2' WHERE k = 1 AND c = 2 IF EXISTS");
    }

    @Test
    public void testDelete() throws Throwable
    {
        testQuery("DELETE FROM %s WHERE k=1");
        testLWTQuery("DELETE FROM %s WHERE k=1 AND c=2 IF EXISTS");
    }

    @Test
    public void testBatch() throws Throwable
    {
        testQuery("BEGIN BATCH INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') APPLY BATCH");
        testQuery("BEGIN BATCH UPDATE %s SET v = 'val2' WHERE k = 1 AND c = 2 APPLY BATCH");
        testQuery("BEGIN BATCH DELETE FROM %s WHERE k=1 APPLY BATCH");

        testLWTQuery("BEGIN BATCH INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') IF NOT EXISTS APPLY BATCH");
        testLWTQuery("BEGIN BATCH UPDATE %s SET v = 'val2' WHERE k = 1 AND c = 2 IF EXISTS APPLY BATCH");
        testLWTQuery("BEGIN BATCH DELETE FROM %s WHERE k=1 AND c=2 IF EXISTS APPLY BATCH");
    }

    private void testQuery(String query) throws Throwable
    {
        testQuery(query, ONE);
        testQuery(query, ALL);
        testQuery(query, ANY);
        testQuery(query, QUORUM);
        testQuery(query, LOCAL_ONE);
        testQuery(query, LOCAL_QUORUM);
    }

    private void testQuery(String query, ConsistencyLevel cl) throws Throwable
    {
        warnConsistencyLevels();
        disableConsistencyLevels();
        assertValid(query, cl, null);

        warnConsistencyLevels(cl);
        assertWarns(query, cl, null, cl);

        warnConsistencyLevels();
        disableConsistencyLevels(cl);
        assertFails(query, cl, null, cl);
    }

    private void testLWTQuery(String query) throws Throwable
    {
        testLWTQuery(query, ONE);
        testLWTQuery(query, ALL);
        testLWTQuery(query, QUORUM);
        testLWTQuery(query, LOCAL_ONE);
        testLWTQuery(query, LOCAL_QUORUM);
    }

    private void testLWTQuery(String query, ConsistencyLevel cl) throws Throwable
    {
        disableConsistencyLevels();

        warnConsistencyLevels();
        assertValid(query, cl, SERIAL);
        assertValid(query, cl, LOCAL_SERIAL);
        assertValid(query, cl, null);

        warnConsistencyLevels(cl);
        assertWarns(query, cl, SERIAL, cl);
        assertWarns(query, cl, LOCAL_SERIAL, cl);
        assertWarns(query, cl, null, cl);

        warnConsistencyLevels(SERIAL);
        assertWarns(query, cl, SERIAL, SERIAL);
        assertValid(query, cl, LOCAL_SERIAL);
        assertWarns(query, cl, null, SERIAL);

        warnConsistencyLevels(LOCAL_SERIAL);
        assertValid(query, cl, SERIAL);
        assertWarns(query, cl, LOCAL_SERIAL, LOCAL_SERIAL);
        assertValid(query, cl, null);

        warnConsistencyLevels(SERIAL, LOCAL_SERIAL);
        assertWarns(query, cl, SERIAL, SERIAL);
        assertWarns(query, cl, LOCAL_SERIAL, LOCAL_SERIAL);
        assertWarns(query, cl, null, SERIAL);

        warnConsistencyLevels();

        disableConsistencyLevels(cl);
        assertFails(query, cl, SERIAL, cl);
        assertFails(query, cl, LOCAL_SERIAL, cl);
        assertFails(query, cl, null, cl);

        disableConsistencyLevels(SERIAL);
        assertFails(query, cl, SERIAL, SERIAL);
        assertValid(query, cl, LOCAL_SERIAL);
        assertFails(query, cl, null, SERIAL);

        disableConsistencyLevels(LOCAL_SERIAL);
        assertValid(query, cl, SERIAL);
        assertFails(query, cl, LOCAL_SERIAL, LOCAL_SERIAL);
        assertValid(query, cl, null);

        disableConsistencyLevels(SERIAL, LOCAL_SERIAL);
        assertFails(query, cl, SERIAL, SERIAL);
        assertFails(query, cl, LOCAL_SERIAL, LOCAL_SERIAL);
        assertFails(query, cl, null, SERIAL);
    }

    private void assertValid(String query, ConsistencyLevel cl, ConsistencyLevel serialCl) throws Throwable
    {
        assertValid(() -> execute(userClientState, query, cl, serialCl));
    }

    private void assertWarns(String query, ConsistencyLevel cl, ConsistencyLevel serialCl, ConsistencyLevel warnedCl) throws Throwable
    {
        assertWarns(() -> execute(userClientState, query, cl, serialCl),
                    format("Provided values [%s] are not recommended for write consistency levels (warned values are: %s)",
                           warnedCl, guardrails().getWriteConsistencyLevelsWarned()));

        assertExcludedUsers(query, cl, serialCl);
    }

    private void assertFails(String query, ConsistencyLevel cl, ConsistencyLevel serialCl, ConsistencyLevel rejectedCl) throws Throwable
    {
        assertFails(() -> execute(userClientState, query, cl, serialCl),
                    format("Provided values [%s] are not allowed for write consistency levels (disallowed values are: %s)",
                           rejectedCl, guardrails().getWriteConsistencyLevelsDisallowed()));

        assertExcludedUsers(query, cl, serialCl);
    }

    private void assertExcludedUsers(String query, ConsistencyLevel cl, ConsistencyLevel serialCl) throws Throwable
    {
        assertValid(() -> execute(superClientState, query, cl, serialCl));
        assertValid(() -> execute(systemClientState, query, cl, serialCl));
    }
}
