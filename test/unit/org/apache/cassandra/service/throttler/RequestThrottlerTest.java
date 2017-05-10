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

package org.apache.cassandra.service.throttler;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.RequestThrottledException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.transport.ProtocolVersion;

public class RequestThrottlerTest extends CQLTester
{
    protected static final Logger logger = LoggerFactory.getLogger(RequestThrottlerTest.class);
    private final ProtocolVersion protocolVersion = ProtocolVersion.CURRENT;
    private KeyspaceBasedRequestThrottler keyspaceThrottler;

    public RequestThrottlerTest()
    {
        requireNetwork();
    }

    private ResultSet exec(String query, Object... values) throws Throwable
    {
        return executeNet(protocolVersion, query, values);
    }

    private void assertWriteThrottled(String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(Optional.of(protocolVersion), "", WriteTimeoutException.class, query, values);
    }

    private void assertReadThrottled(String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(Optional.of(protocolVersion), "", ReadTimeoutException.class, query, values);
    }

    private void assertLwtThrottled(String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(Optional.of(protocolVersion), "", ReadTimeoutException.class, query, values);
    }

    @Test
    public void testNoOpThrottler() throws Throwable
    {
        DatabaseDescriptor.setRequestThrottler(new NoOpRequestThrottler(Collections.emptyMap()));

        createTable("CREATE TABLE %s (key1 text, key2 text, val1 int, val2 text, PRIMARY KEY(key1, key2))");

        exec("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?)", "a1", "a2", 1, "a3");
        exec("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?)", "b1", "b2", 2, "b3");
        assertRowsNet(protocolVersion, exec("SELECT key1, key2, val1, val2 FROM %s WHERE key1 = ?", "a1"),
                      row("a1", "a2", 1, "a3"));

        exec("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?) IF NOT EXISTS", "a1", "a7", 3, "x");
        Assert.assertEquals(2, exec("SELECT * FROM %s WHERE key1 = ?", "a1").all().size());
    }

    @Test
    public void testBlockAllThrottler() throws Throwable
    {
        // Create a new throttler that blocks all requests to the test keyspace.
        DatabaseDescriptor.setRequestThrottler(new IRequestThrottler()
        {
            public void setup()
            {
            }

            public void maybeThrottleRead(ReadCommand command, ConsistencyLevel consistencyLevel) throws RequestThrottledException
            {
                if (command.metadata().keyspace.equals(KEYSPACE))
                {
                    throw new RequestThrottledException("Always throttle read");
                }
            }

            public void maybeThrottleMutation(IMutation mutation, ConsistencyLevel consistencyLevel) throws RequestThrottledException
            {
                if (mutation.getKeyspaceName().equals(KEYSPACE))
                {
                    throw new RequestThrottledException("Always throttle mutation");
                }
            }
        });

        createTable("CREATE TABLE %s (key1 text, key2 text, val1 int, val2 text, PRIMARY KEY(key1, key2))");

        assertWriteThrottled("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?)", "a1", "a2", 1, "a3");
        assertWriteThrottled("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?)", "b1", "b2", 2, "b3");
        assertReadThrottled("SELECT key1, key2, val1, val2 FROM %s WHERE key1 = ?", "a1");
        assertLwtThrottled("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?) IF NOT EXISTS", "a1", "a7", 3, "x");
    }

    private void initializeKeyspaceBasedThrottler() throws Throwable
    {
        Map<String, String> params = new HashMap<String, String>()
        {{
            put("fetch_limits_period_in_sec", "100");
            put("replenish_limits_period_in_sec", "100");
        }};

        keyspaceThrottler = new KeyspaceBasedRequestThrottler(params);
        try
        {
            Schema.instance.transform(SchemaTransformations.updateSystemKeyspace(
                                      keyspaceThrottler.getLimitProvider().getKeyspaceMetadata(), 0));
        }
        catch (AlreadyExistsException e)
        {
            logger.debug("Attempted to create new keyspace, but it already exists");

            // Clear old limits from previous tests.
            exec("TRUNCATE system_throttle.limits");
        }

        keyspaceThrottler.setup();
        DatabaseDescriptor.setRequestThrottler(keyspaceThrottler);
    }

    @Test
    public void testKeyspaceBasedThrottler_fetchKeyspaceLimits() throws Throwable
    {
        initializeKeyspaceBasedThrottler();

        exec("INSERT INTO system_throttle.limits (partition, keyspace_name, range_read_limit, serial_mutation_limit, serial_read_limit, single_mutation_limit, single_read_limit) " +
             "VALUES (0, 'keyspace_test', 1, 2, 3, 4, 5);");
        keyspaceThrottler.fetchLimitsFromProvider();

        Map<String, KeyspaceLimits> allFetchedLimits = keyspaceThrottler.getFetchedKeyspaceLimits();
        Assert.assertEquals(1, allFetchedLimits.size());
        KeyspaceLimits fetchedLimits = allFetchedLimits.get("keyspace_test");
        Assert.assertNotNull(fetchedLimits);
        Assert.assertEquals(1, fetchedLimits.rangeReadLimit.get());
        Assert.assertEquals(2, fetchedLimits.serialMutationLimit.get());
        Assert.assertEquals(3, fetchedLimits.serialReadLimit.get());
        Assert.assertEquals(4, fetchedLimits.singleMutationLimit.get());
        Assert.assertEquals(5, fetchedLimits.singleReadLimit.get());
    }

    @Test
    public void testKeyspaceBasedThrottler_respectLimits() throws Throwable
    {
        initializeKeyspaceBasedThrottler();

        exec("INSERT INTO system_throttle.limits (partition, keyspace_name, range_read_limit, serial_mutation_limit, serial_read_limit, single_mutation_limit, single_read_limit) " +
             "VALUES (0, ?, 0, 0, 0, 0, 0);", KEYSPACE);
        keyspaceThrottler.fetchLimitsFromProvider();
        keyspaceThrottler.replenishLocalLimits();

        createTable("CREATE TABLE %s (key1 text, key2 text, val1 int, val2 text, PRIMARY KEY(key1, key2))");

        assertWriteThrottled("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?)", "a1", "a2", 1, "a3");
        assertWriteThrottled("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?)", "b1", "b2", 2, "b3");
        assertReadThrottled("SELECT key1, key2, val1, val2 FROM %s WHERE key1 = ?", "a1");
        assertLwtThrottled("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?) IF NOT EXISTS", "a1", "a7", 3, "x");

        exec("INSERT INTO system_throttle.limits (partition, keyspace_name, range_read_limit, serial_mutation_limit, serial_read_limit, single_mutation_limit, single_read_limit) " +
             "VALUES (0, ?, 10, 10, 10, 10, 10);", KEYSPACE);
        keyspaceThrottler.fetchLimitsFromProvider();
        keyspaceThrottler.replenishLocalLimits();

        exec("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?)", "a1", "a2", 1, "a3");
        Assert.assertEquals(9, keyspaceThrottler.getCurrentKeyspaceLimits().get(KEYSPACE).singleMutationLimit.get());

        keyspaceThrottler.replenishLocalLimits();
        Assert.assertEquals(10, keyspaceThrottler.getCurrentKeyspaceLimits().get(KEYSPACE).singleMutationLimit.get());
        exec("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?)", "b1", "b2", 2, "b3");
        Assert.assertEquals(9, keyspaceThrottler.getCurrentKeyspaceLimits().get(KEYSPACE).singleMutationLimit.get());

        keyspaceThrottler.replenishLocalLimits();
        assertRowsNet(protocolVersion, exec("SELECT key1, key2, val1, val2 FROM %s WHERE key1 = ?", "a1"),
                      row("a1", "a2", 1, "a3"));
        Assert.assertEquals(9, keyspaceThrottler.getCurrentKeyspaceLimits().get(KEYSPACE).singleReadLimit.get());

        keyspaceThrottler.replenishLocalLimits();
        exec("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?) IF NOT EXISTS", "a1", "a7", 3, "x");
        Assert.assertEquals(10, keyspaceThrottler.getCurrentKeyspaceLimits().get(KEYSPACE).singleMutationLimit.get());
        Assert.assertEquals(9, keyspaceThrottler.getCurrentKeyspaceLimits().get(KEYSPACE).serialReadLimit.get());


        keyspaceThrottler.replenishLocalLimits();
        Assert.assertEquals(2, exec("SELECT * FROM %s WHERE key1 = ?", "a1").all().size());
        Assert.assertEquals(9, keyspaceThrottler.getCurrentKeyspaceLimits().get(KEYSPACE).singleReadLimit.get());

        keyspaceThrottler.replenishLocalLimits();
        Assert.assertEquals(3, exec("SELECT * FROM %s").all().size());
        Assert.assertEquals(7, keyspaceThrottler.getCurrentKeyspaceLimits().get(KEYSPACE).rangeReadLimit.get());
    }

    @Test
    public void testKeyspaceBasedThrottler_hasDefaultLimits() throws Throwable
    {
        initializeKeyspaceBasedThrottler();

        exec("INSERT INTO system_throttle.limits (partition, keyspace_name, range_read_limit, serial_mutation_limit, serial_read_limit, single_mutation_limit, single_read_limit) " +
             "VALUES (0, ?, 1, 1, 1, 1, 1);", KeyspaceBasedRequestThrottler.DEFAULT_PER_KEYSPACE_LIMIT_KEY);
        keyspaceThrottler.fetchLimitsFromProvider();
        keyspaceThrottler.replenishLocalLimits();

        Assert.assertEquals(1, keyspaceThrottler.getCurrentKeyspaceLimits().get(KEYSPACE).singleMutationLimit.get());
        createTable("CREATE TABLE %s (key1 text, key2 text, val1 int, val2 text, PRIMARY KEY(key1, key2))");
        exec("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?)", "a1", "a2", 1, "a3");
        assertWriteThrottled("INSERT INTO %s (key1, key2, val1, val2) values (?, ?, ?, ?)", "a1", "a2", 1, "a3");

        // If we create a new keyspace, it automatically has the default limits.
        String newKeyspace = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        keyspaceThrottler.fetchLimitsFromProvider();
        keyspaceThrottler.replenishLocalLimits();
        Assert.assertEquals(1, keyspaceThrottler.getCurrentKeyspaceLimits().get(newKeyspace).singleMutationLimit.get());
    }

    @Test
    public void testKeyspaceBasedThrottler_mergesDefaultLimits() throws Throwable
    {
        initializeKeyspaceBasedThrottler();

        exec("INSERT INTO system_throttle.limits (partition, keyspace_name, range_read_limit, serial_mutation_limit, serial_read_limit, single_mutation_limit, single_read_limit) " +
             "VALUES (0, ?, 1, 1, 1, 1, 1);", KeyspaceBasedRequestThrottler.DEFAULT_PER_KEYSPACE_LIMIT_KEY);
        keyspaceThrottler.fetchLimitsFromProvider();
        keyspaceThrottler.replenishLocalLimits();
        Assert.assertEquals(1, keyspaceThrottler.getCurrentKeyspaceLimits().get(KEYSPACE).singleMutationLimit.get());

        createTable("CREATE TABLE %s (key1 text, key2 text, val1 int, val2 text, PRIMARY KEY(key1, key2))");
        exec("INSERT INTO system_throttle.limits (partition, keyspace_name, range_read_limit, serial_mutation_limit, serial_read_limit, single_mutation_limit, single_read_limit) " +
             "VALUES (0, ?, 3, 3, 3, 3, 3);", KEYSPACE);
        keyspaceThrottler.fetchLimitsFromProvider();
        keyspaceThrottler.replenishLocalLimits();
        Assert.assertEquals(3, keyspaceThrottler.getCurrentKeyspaceLimits().get(KEYSPACE).singleMutationLimit.get());
    }
}
