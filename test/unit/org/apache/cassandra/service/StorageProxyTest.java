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

package org.apache.cassandra.service;

import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.StorageMetrics;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(BMUnitRunner.class)
@BMUnitConfig(verbose=true, bmunitVerbose=true)
public class StorageProxyTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.mkdirs();
    }

    @Test
    public void testSetGetPaxosVariant()
    {
        assertEquals(Config.PaxosVariant.v1, DatabaseDescriptor.getPaxosVariant());
        assertEquals("v1", StorageProxy.instance.getPaxosVariant());
        StorageProxy.instance.setPaxosVariant("v2");
        assertEquals("v2", StorageProxy.instance.getPaxosVariant());
        assertEquals(Config.PaxosVariant.v2, DatabaseDescriptor.getPaxosVariant());
        DatabaseDescriptor.setPaxosVariant(Config.PaxosVariant.v1);
        assertEquals(Config.PaxosVariant.v1, DatabaseDescriptor.getPaxosVariant());
        assertEquals(Config.PaxosVariant.v1, DatabaseDescriptor.getPaxosVariant());
    }

    @Test
    public void testShouldHint() throws Exception
    {
        // HAPPY PATH with all defaults
        shouldHintTest(replica -> {
            assertThat(StorageProxy.shouldHint(replica)).isTrue();
            assertThat(StorageProxy.shouldHint(replica, /* tryEnablePersistentWindow */ false)).isTrue();
        });
    }

    @Test
    public void testShouldHintOnWindowExpiry() throws Exception
    {
        shouldHintTest(replica -> {
            // wait for 5 ms, we will shorten the hints window later
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);

            final int originalHintWindow = DatabaseDescriptor.getMaxHintWindow();
            try
            {
                DatabaseDescriptor.setMaxHintWindow(1); // 1 ms. It should not hint
                assertThat(StorageProxy.shouldHint(replica)).isFalse();
            }
            finally
            {
                DatabaseDescriptor.setMaxHintWindow(originalHintWindow);
            }
        });
    }

    @Test
    @BMRule(name = "Hints size exceeded the limit",
            targetClass="org.apache.cassandra.hints.HintsService",
            targetMethod="getTotalHintsSize",
            action="return 2097152;") // 2MB
    public void testShouldHintOnExceedingSize() throws Exception
    {
        shouldHintTest(replica -> {
            final int originalHintsSizeLimit = DatabaseDescriptor.getMaxHintsSizePerHostInMiB();
            try
            {
                DatabaseDescriptor.setMaxHintsSizePerHostInMiB(1);
                assertThat(StorageProxy.shouldHint(replica)).isFalse();
            }
            finally
            {
                DatabaseDescriptor.setMaxHintsSizePerHostInMiB(originalHintsSizeLimit);
            }
        });
    }


    /**
     * Ensure that the timer backing the JMX endpoint to transiently enable blocking read repairs both enables
     * and disables the way we'd expect.
     */
    @Test
    public void testTransientLoggingTimer()
    {
        StorageProxy.instance.logBlockingReadRepairAttemptsForNSeconds(2);
        Assert.assertTrue(StorageProxy.instance.isLoggingReadRepairs());
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        Assert.assertFalse(StorageProxy.instance.isLoggingReadRepairs());
    }

    private void shouldHintTest(Consumer<Replica> test) throws UnknownHostException
    {
        InetAddressAndPort testEp = InetAddressAndPort.getByName("192.168.1.1");
        Replica replica = full(testEp);
        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), testEp);
        EndpointState state = new EndpointState(new HeartBeatState(0, 0));
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.markDead(replica.endpoint(), state));

        try
        {
            test.accept(replica);
        }
        finally
        {
            StorageService.instance.getTokenMetadata().removeEndpoint(testEp);
        }
    }

    public static AtomicBoolean hintsHit = new AtomicBoolean(false);
    public static AtomicBoolean mutationsHit = new AtomicBoolean(false);
    @Test
    @BMRules(rules = {
        @BMRule(name = "Separate hints",
        targetClass = "org.apache.cassandra.concurrent.Stage",
        targetLocation = "ENTRY",
        targetMethod = "submit(Runnable)",
        condition = "$0.name().equals(Stage.HINT.name())",
        action = "org.apache.cassandra.service.StorageProxyTest.hintsHit.set(true);"),
        @BMRule(name = "Separate mutations",
        targetClass = "org.apache.cassandra.concurrent.Stage",
        targetLocation = "ENTRY",
        targetMethod = "submit(Runnable)",
        condition = "$0.name().equals(Stage.MUTATION.name())",
        action = "org.apache.cassandra.service.StorageProxyTest.mutationsHit.set(true);")
    })
    public void testHintIsPutByDefaultInHintQueueAndNotMutationQueue() throws Exception {
        // Mock mutation and create a real replica
        Mutation mockMutation = mock(Mutation.class);
        Replica realReplica = createRealReplica();

        // Mock the response handler
        AbstractWriteResponseHandler<IMutation> mockResponseHandler = mock(AbstractWriteResponseHandler.class);

        // Capture the metrics before the method call
        long hintsBefore = StorageMetrics.totalHintsInProgress.getCount();

        // Invoke the submitHint method
        StorageProxy.submitHint(mockMutation, realReplica, mockResponseHandler);

        // Capture the metrics after the method call
        long hintsAfter = StorageMetrics.totalHintsInProgress.getCount();

        // Assertions
        assertEquals(1, hintsAfter - hintsBefore);
        assertEquals(true, hintsHit.get());
        assertEquals(false, mutationsHit.get());
    }

    private Replica createRealReplica() throws UnknownHostException {
        InetAddressAndPort inetAddress = InetAddressAndPort.getByName("127.0.0.1");
        // Create a Token and Range<Token>
        IPartitioner partitioner = Murmur3Partitioner.instance;
        Token startToken = partitioner.getMinimumToken();
        Token endToken = partitioner.getRandomToken();
        Range<Token> tokenRange = new Range<>(startToken, endToken);

        return Replica.fullReplica(inetAddress, tokenRange);
    }
}
