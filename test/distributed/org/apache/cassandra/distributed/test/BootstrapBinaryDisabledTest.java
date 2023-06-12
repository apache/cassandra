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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.LogResult;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.Byteman;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.config.CassandraRelevantProperties.RING_DELAY;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_WRITE_SURVEY;

/**
 * Replaces python dtest bootstrap_test.py::TestBootstrap::test_bootstrap_binary_disabled
 */
public class BootstrapBinaryDisabledTest extends TestBaseImpl
{
    static WithProperties properties;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        TestBaseImpl.beforeClass();
        properties = new WithProperties().set(RESET_BOOTSTRAP_PROGRESS, false);
    }

    @AfterClass
    public static void afterClass()
    {
        properties.close();
    }

    @Test
    public void test() throws IOException, TimeoutException
    {
        Map<String, Object> config = new HashMap<>();
        config.put("authenticator", "org.apache.cassandra.auth.PasswordAuthenticator");
        config.put("authorizer", "org.apache.cassandra.auth.CassandraAuthorizer");
        config.put("role_manager", "org.apache.cassandra.auth.CassandraRoleManager");
        config.put("permissions_validity", "0ms");
        config.put("roles_validity", "0ms");

        int originalNodeCount = 1;
        int expandedNodeCount = originalNodeCount + 2;
        Byteman byteman = Byteman.createFromScripts("test/resources/byteman/stream_failure.btm");
        try (Cluster cluster = init(Cluster.build(originalNodeCount)
                                           .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                           .withConfig(c -> {
                                               config.forEach(c::set);
                                               c.with(Feature.GOSSIP, Feature.NETWORK, Feature.NATIVE_PROTOCOL);
                                           })
                                           .withInstanceInitializer((cl, nodeNumber) -> {
                                               switch (nodeNumber) {
                                                   case 1:
                                                   case 2:
                                                       byteman.install(cl);
                                                       break;
                                               }
                                           })
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk text primary key)");
            populate(cluster.get(1));
            cluster.forEach(c -> c.flush(KEYSPACE));

            bootstrap(cluster, config, false);
            // Test write survey behaviour
            bootstrap(cluster, config, true);
        }
    }

    private static void bootstrap(Cluster cluster,
                                  Map<String, Object> config,
                                  boolean isWriteSurvey) throws TimeoutException
    {
        IInstanceConfig nodeConfig = cluster.newInstanceConfig();
        nodeConfig.set("auto_bootstrap", true);
        config.forEach(nodeConfig::set);

        //TODO can we make this more isolated?
        RING_DELAY.setLong(5000);
        if (isWriteSurvey)
            TEST_WRITE_SURVEY.setBoolean(true);

        RewriteEnabled.enable();
        cluster.bootstrap(nodeConfig).startup();
        IInvokableInstance node = cluster.get(cluster.size());
        assertLogHas(node, "Some data streaming failed");
        assertLogHas(node, isWriteSurvey ?
                           "Not starting client transports in write_survey mode as it's bootstrapping or auth is enabled" :
                           "Node is not yet bootstrapped completely");

        node.nodetoolResult("join").asserts()
            .failure()
            .errorContains("Cannot join the ring until bootstrap completes");

        node.nodetoolResult("bootstrap", "resume").asserts().failure();
        RewriteEnabled.disable();
        node.nodetoolResult("bootstrap", "resume").asserts().success();
        if (isWriteSurvey)
            assertLogHas(node, "Not starting client transports in write_survey mode as it's bootstrapping or auth is enabled");

        if (isWriteSurvey)
        {
            node.nodetoolResult("join").asserts().success();
            assertLogHas(node, "Leaving write survey mode and joining ring at operator request");
        }

        node.logs().watchFor("Starting listening for CQL clients");
        assertBootstrapState(node, "COMPLETED");
    }

    private static void assertBootstrapState(IInvokableInstance node, String expected)
    {
        SimpleQueryResult qr = node.executeInternalWithResult("SELECT bootstrapped FROM system.local WHERE key='local'");
        Assert.assertTrue("No rows found", qr.hasNext());
        Assert.assertEquals(expected, qr.next().getString("bootstrapped"));
    }

    private static void assertLogHas(IInvokableInstance node, String msg)
    {
        LogResult<List<String>> results = node.logs().grep(msg);
        Assert.assertFalse("Unable to find '" + msg + "'", results.getResult().isEmpty());
    }

    private void populate(IInvokableInstance inst)
    {
        for (int i = 0; i < 10; i++)
            inst.executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)", Integer.toString(i));
    }

    @Shared
    public static final class RewriteEnabled
    {
        private static volatile boolean enabled = false;

        public static boolean isEnabled()
        {
            return enabled;
        }

        public static void enable()
        {
            enabled = true;
        }

        public static void disable()
        {
            enabled = false;
        }
    }
}
