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
import java.util.function.Supplier;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;

public class GuardrailConsistencyTest extends GuardrailTester
{
    private static Set<String> disallowedConsistencyLevels = ImmutableSet.of(
    ConsistencyLevel.ANY.toString(),
    ConsistencyLevel.ONE.toString(),
    ConsistencyLevel.TWO.toString(),
    ConsistencyLevel.THREE.toString(),
    ConsistencyLevel.QUORUM.toString(),
    ConsistencyLevel.ALL.toString(),
    ConsistencyLevel.EACH_QUORUM.toString(),
    ConsistencyLevel.LOCAL_ONE.toString()
    );
    private static Set<String> serialConsistencyLevels = ImmutableSet.of(
    ConsistencyLevel.SERIAL.toString(),
    ConsistencyLevel.LOCAL_SERIAL.toString()
    );

    private static Set<String> defaultDisallowedWriteConsistencyLevels;
    private Supplier<QueryState> queryState;

    @BeforeClass
    public static void setup()
    {
        defaultDisallowedWriteConsistencyLevels = DatabaseDescriptor.getGuardrailsConfig().write_consistency_levels_disallowed;
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().write_consistency_levels_disallowed = defaultDisallowedWriteConsistencyLevels;
    }

    @Before
    public void setupTest()
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
        queryState = this::userQueryState;
        disableConsistencyLevels(disallowedConsistencyLevels);
    }

    private void disableConsistencyLevels(Set<String> consistencyLevels)
    {
        DatabaseDescriptor.getGuardrailsConfig().write_consistency_levels_disallowed = ImmutableSet.copyOf(consistencyLevels);
    }

    private QueryOptions queryOptions(ConsistencyLevel cl, ConsistencyLevel serialCl)
    {
        return QueryOptions.create(cl,
                                   Collections.emptyList(),
                                   false,
                                   PageSize.inRows(1),
                                   null,
                                   serialCl,
                                   ProtocolVersion.CURRENT,
                                   KEYSPACE);
    }

    private void executeWithConsistency(String query, ConsistencyLevel cl, ConsistencyLevel serialCl)
    {
        QueryOptions queryOptions = queryOptions(cl, serialCl);
        QueryState state = queryState.get();
        CQLStatement statement = QueryProcessor.getStatement(formatQuery(query), state.getClientState());
        statement.execute(state, queryOptions, System.nanoTime());
    }

    private void insert(ConsistencyLevel cl)
    {
        executeWithConsistency("INSERT INTO %s (k, c, v) VALUES (1, 2, 'val')", cl, null);
    }

    private void lwtInsert(ConsistencyLevel cl, ConsistencyLevel serialCl)
    {
        executeWithConsistency("INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') IF NOT EXISTS", cl, serialCl);
    }

    @Test(expected = InvalidRequestException.class)
    public void testInsertWithDisallowedConsistency()
    {
        insert(ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testLWTInsertWithDisallowedConsistency1()
    {
        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.SERIAL.toString()));
        lwtInsert(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
    }

    @Test(expected = InvalidRequestException.class)
    public void testLWTInsertWithDisallowedConsistency2()
    {
        disableConsistencyLevels(serialConsistencyLevels);
        lwtInsert(ConsistencyLevel.LOCAL_QUORUM, null);
    }

    @Test
    public void testInsertWithAllowedConsistency()
    {
        // test that it does not throw
        insert(ConsistencyLevel.LOCAL_QUORUM);

        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.SERIAL.toString()));
        lwtInsert(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL);
        lwtInsert(ConsistencyLevel.LOCAL_QUORUM, null);

        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.LOCAL_SERIAL.toString()));
        lwtInsert(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
        lwtInsert(ConsistencyLevel.LOCAL_QUORUM, null);
    }

    private void update(ConsistencyLevel cl)
    {
        executeWithConsistency("UPDATE %s SET v = 'val2' WHERE k = 1 and c = 2", cl, null);
    }

    private void lwtUpdate(ConsistencyLevel cl, ConsistencyLevel serialCl)
    {
        executeWithConsistency("UPDATE %s SET v = 'val2' WHERE k = 1 and c = 2 IF EXISTS", cl, serialCl);
    }

    @Test(expected = InvalidRequestException.class)
    public void testUpdateWithDisallowedConsistency()
    {
        update(ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testLWTUpdateWithDisallowedConsistency1()
    {
        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.SERIAL.toString()));
        lwtUpdate(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
    }

    @Test(expected = InvalidRequestException.class)
    public void testLWTUpdateWithDisallowedConsistency2()
    {
        disableConsistencyLevels(serialConsistencyLevels);
        lwtUpdate(ConsistencyLevel.LOCAL_QUORUM, null);
    }

    @Test
    public void testUpdateWithAllowedConsistency()
    {
        // test that it does not throw
        update(ConsistencyLevel.LOCAL_QUORUM);

        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.SERIAL.toString()));
        lwtUpdate(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL);
        lwtUpdate(ConsistencyLevel.LOCAL_QUORUM, null);

        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.LOCAL_SERIAL.toString()));
        lwtUpdate(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
        lwtUpdate(ConsistencyLevel.LOCAL_QUORUM, null);
    }

    private void delete(ConsistencyLevel cl)
    {
        executeWithConsistency("DELETE FROM %s WHERE k=1", cl, null);
    }

    private void lwtDelete(ConsistencyLevel cl, ConsistencyLevel serialCl)
    {
        executeWithConsistency("DELETE FROM %s WHERE k=1 AND c=2 IF EXISTS", cl, serialCl);
    }

    @Test(expected = InvalidRequestException.class)
    public void testDeleteWithDisallowedConsistency()
    {
        delete(ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testLWTDeleteWithAllowedConsistency1()
    {
        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.SERIAL.toString()));
        lwtDelete(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
    }

    @Test(expected = InvalidRequestException.class)
    public void testLWTDeleteWithAllowedConsistency2()
    {
        disableConsistencyLevels(serialConsistencyLevels);
        lwtDelete(ConsistencyLevel.LOCAL_QUORUM, null);
    }

    @Test
    public void testDeleteWithAllowedConsistency()
    {
        // test that it does not throw
        delete(ConsistencyLevel.LOCAL_QUORUM);

        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.SERIAL.toString()));
        lwtDelete(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL);
        lwtDelete(ConsistencyLevel.LOCAL_QUORUM, null);

        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.LOCAL_SERIAL.toString()));
        lwtDelete(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
        lwtDelete(ConsistencyLevel.LOCAL_QUORUM, null);
    }

    private void batch(ConsistencyLevel cl)
    {
        executeWithConsistency("BEGIN BATCH " +
                               "INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') " +
                               "APPLY BATCH", cl, null);
    }

    private void lwtBatch(ConsistencyLevel cl, ConsistencyLevel serialCl)
    {
        executeWithConsistency("BEGIN BATCH " +
                               "INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') IF NOT EXISTS " +
                               "APPLY BATCH", cl, serialCl);
    }

    @Test(expected = InvalidRequestException.class)
    public void testBatchWithDisallowedConsistency()
    {
        batch(ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testLWTBatchWithDisallowedConsistency1()
    {
        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.SERIAL.toString()));
        lwtBatch(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
    }

    @Test(expected = InvalidRequestException.class)
    public void testLWTBatchWithDisallowedConsistency2()
    {
        disableConsistencyLevels(serialConsistencyLevels);
        lwtBatch(ConsistencyLevel.LOCAL_QUORUM, null);
    }
    @Test
    public void testBatchWithAllowedConsistency()
    {
        // test that it does not throw
        batch(ConsistencyLevel.LOCAL_QUORUM);

        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.SERIAL.toString()));
        lwtBatch(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL);
        lwtBatch(ConsistencyLevel.LOCAL_QUORUM, null);

        disableConsistencyLevels(ImmutableSet.of(ConsistencyLevel.LOCAL_SERIAL.toString()));
        lwtBatch(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
        lwtBatch(ConsistencyLevel.LOCAL_QUORUM, null);
    }

    @Test
    public void testSuperUser()
    {
        queryState = this::superQueryState;
        testExcludedUser();
    }

    @Test
    public void testSystemUser()
    {
        queryState = this::internalQueryState;
        testExcludedUser();
    }

    private void testExcludedUser()
    {
        disableConsistencyLevels(Sets.union(defaultDisallowedWriteConsistencyLevels, serialConsistencyLevels));
        insert(ConsistencyLevel.ONE);
        insert(ConsistencyLevel.LOCAL_QUORUM);
        lwtInsert(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
        lwtInsert(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL);

        update(ConsistencyLevel.ONE);
        update(ConsistencyLevel.LOCAL_QUORUM);
        lwtUpdate(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
        lwtUpdate(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL);

        delete(ConsistencyLevel.ONE);
        delete(ConsistencyLevel.LOCAL_QUORUM);
        lwtDelete(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
        lwtDelete(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL);

        batch(ConsistencyLevel.ONE);
        batch(ConsistencyLevel.LOCAL_QUORUM);
        lwtBatch(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL);
        lwtBatch(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL);
    }
}
