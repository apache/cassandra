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

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config.CLEnforcementLevel;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.metrics.StorageProxyMetricsManager;

import static java.lang.String.format;
import static junit.framework.TestCase.assertEquals;
import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.db.ConsistencyLevel.ANY;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;

public class GuardrailWriteConsistencyEnforcementTest extends GuardrailConsistencyLevelsTester
{
    private long noneEnforcementCount = 0;
    private long softEnforcementCount = 0;
    private long hardEnforcementCount = 0;

    public GuardrailWriteConsistencyEnforcementTest()
    {
        super("write_consistency_enforcement_none",
              "write_consistency_enforcement_hard",
              Guardrails.writeConsistencyEnforcement,
              Guardrails::getWriteConsistencyEnforcementNone,
              Guardrails::getWriteConsistencyEnforcementHard,
              Guardrails::getWriteConsistencyEnforcementNoneCSV,
              Guardrails::getWriteConsistencyEnforcementHardCSV,
              Guardrails::setWriteConsistencyEnforcementNone,
              Guardrails::setWriteConsistencyEnforcementHard,
              Guardrails::setWriteConsistencyEnforcementNoneCSV,
              Guardrails::setWriteConsistencyEnforcementHardCSV);
    }

    @BeforeClass
    public static void beforeClass()
    {
        Guardrails.writeConsistencyEnforcement.minNotifyIntervalInMs(0);
    }

    @Before
    public void before()
    {
        super.before();
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
    }

    @After
    public void after()
    {
        warnConsistencyLevels();
        ignoreConsistencyLevels();
        disableConsistencyLevels();
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

    private void ignoreConsistencyLevels(ConsistencyLevel... consistencyLevels)
    {
        guardrails().setWriteConsistencyEnforcementSoft(Arrays.stream(consistencyLevels).map(ConsistencyLevel::name).collect(Collectors.toSet()));
    }

    private void testQuery(String query, ConsistencyLevel cl) throws Throwable
    {
        warnConsistencyLevels();
        disableConsistencyLevels();
        ignoreConsistencyLevels();
        assertValid(query, cl, null);

        warnConsistencyLevels(cl);
        assertWarns(query, cl, null, cl);

        warnConsistencyLevels();
        disableConsistencyLevels();
        ignoreConsistencyLevels(cl);
        assertIgnores(query, cl, null, cl);

        warnConsistencyLevels(cl);
        ignoreConsistencyLevels(cl);
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
        ignoreConsistencyLevels();

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
        ignoreConsistencyLevels(cl);
        assertIgnores(query, cl, SERIAL, cl);
        assertIgnores(query, cl, LOCAL_SERIAL, cl);
        assertIgnores(query, cl, null, cl);

        ignoreConsistencyLevels(SERIAL);
        assertIgnores(query, cl, SERIAL, SERIAL);
        assertValid(query, cl, LOCAL_SERIAL);
        assertIgnores(query, cl, null, SERIAL);

        ignoreConsistencyLevels(LOCAL_SERIAL);
        assertValid(query, cl, SERIAL);
        assertIgnores(query, cl, LOCAL_SERIAL, LOCAL_SERIAL);
        assertValid(query, cl, null);

        ignoreConsistencyLevels(SERIAL, LOCAL_SERIAL);
        assertIgnores(query, cl, SERIAL, SERIAL);
        assertIgnores(query, cl, LOCAL_SERIAL, LOCAL_SERIAL);
        assertIgnores(query, cl, null, SERIAL);

        ignoreConsistencyLevels();
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

    private void captureMetrics(ConsistencyLevel cl)
    {
        noneEnforcementCount = StorageProxyMetricsManager.getMetrics(KEYSPACE, cl).writeCLEnforcementMeter.get(CLEnforcementLevel.None).getCount();
        softEnforcementCount = StorageProxyMetricsManager.getMetrics(KEYSPACE, cl).writeCLEnforcementMeter.get(CLEnforcementLevel.Soft).getCount();
        hardEnforcementCount = StorageProxyMetricsManager.getMetrics(KEYSPACE, cl).writeCLEnforcementMeter.get(CLEnforcementLevel.Hard).getCount();
    }

    private void assertMetricsChanged(ConsistencyLevel cl, long noneDelta, long softDelta, long hardDelta)
    {
        assertEquals(noneEnforcementCount + noneDelta, StorageProxyMetricsManager.getMetrics(KEYSPACE, cl).writeCLEnforcementMeter.get(CLEnforcementLevel.None).getCount());
        assertEquals(softEnforcementCount + softDelta, StorageProxyMetricsManager.getMetrics(KEYSPACE, cl).writeCLEnforcementMeter.get(CLEnforcementLevel.Soft).getCount());
        assertEquals(hardEnforcementCount + hardDelta, StorageProxyMetricsManager.getMetrics(KEYSPACE, cl).writeCLEnforcementMeter.get(CLEnforcementLevel.Hard).getCount());
    }

    private void assertValid(String query, ConsistencyLevel cl, ConsistencyLevel serialCl) throws Throwable
    {
        captureMetrics(cl);
        assertValid(() -> execute(userClientState, query, cl, serialCl));
        assertMetricsChanged(cl, 0, 0, 0);
    }

    private void assertWarns(String query, ConsistencyLevel cl, ConsistencyLevel serialCl, ConsistencyLevel warnedCl) throws Throwable
    {
        captureMetrics(cl);
        assertWarns(() -> execute(userClientState, query, cl, serialCl),
                    format("Provided values [%s] are not recommended for write consistency levels (warned values are: %s)",
                           warnedCl, guardrails().getWriteConsistencyEnforcementNone()));

        assertExcludedUsers(query, cl, serialCl);
        assertMetricsChanged(cl, 1, 0, 0);
    }

    private void assertFails(String query, ConsistencyLevel cl, ConsistencyLevel serialCl, ConsistencyLevel rejectedCl) throws Throwable
    {
        captureMetrics(cl);
        assertFails(() -> execute(userClientState, query, cl, serialCl),
                    format("Provided values [%s] are not allowed for write consistency levels (disallowed values are: %s)",
                           rejectedCl, guardrails().getWriteConsistencyEnforcementHard()));

        assertExcludedUsers(query, cl, serialCl);
        assertMetricsChanged(cl, 0, 0, 1);
    }

    private void assertIgnores(String query, ConsistencyLevel cl, ConsistencyLevel serialCl, ConsistencyLevel ignoredCL) throws Throwable
    {
        captureMetrics(cl);
        assertWarns(() -> execute(userClientState, query, cl, serialCl),
                    format("Ignoring provided values [%s] as they are not supported for write consistency levels (ignored values are: %s)",
                           ignoredCL, guardrails().getWriteConsistencyEnforcementSoft()));

        assertExcludedUsers(query, cl, serialCl);
        assertMetricsChanged(cl, 0, 1, 0);
    }

    private void assertExcludedUsers(String query, ConsistencyLevel cl, ConsistencyLevel serialCl) throws Throwable
    {
        assertValid(() -> execute(superClientState, query, cl, serialCl));
        assertValid(() -> execute(systemClientState, query, cl, serialCl));
    }

    @Override
    protected void assertValidProperty(Set<ConsistencyLevel> input)
    {
        super.assertValidProperty(input);
        Set<String> properties = input.stream().map(ConsistencyLevel::name).collect(Collectors.toSet());
        assertValidProperty(Guardrails::setWriteConsistencyEnforcementSoft, Guardrails::getWriteConsistencyEnforcementSoft, properties);
    }

    @Override
    protected void assertValidPropertyCSV(String csv)
    {
        super.assertValidPropertyCSV(csv);
        assertValidProperty(Guardrails::setWriteConsistencyEnforcementSoftCSV, Guardrails::getWriteConsistencyEnforcementSoftCSV, csv);
    }

    @Override
    protected void assertInvalidPropertyCSV(String properties, String rejected)
    {
        super.assertInvalidPropertyCSV(properties, rejected);
        String message = "No enum constant org.apache.cassandra.db.ConsistencyLevel.%s";
        assertInvalidProperty(Guardrails::setWriteConsistencyEnforcementSoftCSV, properties, message, rejected);
    }
}
