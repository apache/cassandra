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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Stage;
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
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(BMUnitRunner.class)
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
        Assert.assertEquals(Config.PaxosVariant.v1, DatabaseDescriptor.getPaxosVariant());
        Assert.assertEquals("v1", StorageProxy.instance.getPaxosVariant());
        StorageProxy.instance.setPaxosVariant("v2");
        Assert.assertEquals("v2", StorageProxy.instance.getPaxosVariant());
        Assert.assertEquals(Config.PaxosVariant.v2, DatabaseDescriptor.getPaxosVariant());
        DatabaseDescriptor.setPaxosVariant(Config.PaxosVariant.v1);
        Assert.assertEquals(Config.PaxosVariant.v1, DatabaseDescriptor.getPaxosVariant());
        Assert.assertEquals(Config.PaxosVariant.v1, DatabaseDescriptor.getPaxosVariant());
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

    private void setExecutors(ExecutorPlus mockHintExecutor, ExecutorPlus mockMutationExecutor) throws Exception {
        Field executorField = Stage.class.getDeclaredField("executor");
        executorField.setAccessible(true);

        // Replace the HINT and MUTATION executors with the mocked ones
        executorField.set(Stage.HINT, mockHintExecutor);
        executorField.set(Stage.MUTATION, mockMutationExecutor);
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

    private void invokeSubmitHint(Mutation mockMutation, Replica realReplica, AbstractWriteResponseHandler<IMutation> mockResponseHandler) throws Exception {
        // Use reflection to access the private method submitHint
        Method submitHintMethod = StorageProxy.class.getDeclaredMethod(
        "submitHint", Mutation.class, Replica.class, AbstractWriteResponseHandler.class);
        submitHintMethod.setAccessible(true);

        // Invoke the private `submitHint` method via reflection
        submitHintMethod.invoke(StorageProxy.instance, mockMutation, realReplica, mockResponseHandler);
    }

    @Test
    public void testHintIsPutByDefaultInHintQueueAndNotMutationQueue() throws Exception {
        // Mock the executors for both HINT and MUTATION stages
        ExecutorPlus mockHintExecutor = mock(ExecutorPlus.class);
        ExecutorPlus mockMutationExecutor = mock(ExecutorPlus.class);

        // Set the mocked executors
        setExecutors(mockHintExecutor, mockMutationExecutor);

        // Mock mutation and create a real replica
        Mutation mockMutation = mock(Mutation.class);
        Replica realReplica = createRealReplica();

        // Mock the response handler
        AbstractWriteResponseHandler<IMutation> mockResponseHandler = mock(AbstractWriteResponseHandler.class);

        // Capture the metrics before the method call
        long hintsBefore = StorageMetrics.totalHintsInProgress.getCount();

        // Invoke the submitHint method
        invokeSubmitHint(mockMutation, realReplica, mockResponseHandler);

        // Capture the metrics after the method call
        long hintsAfter = StorageMetrics.totalHintsInProgress.getCount();

        // Assertions
        Assert.assertEquals(1, hintsAfter - hintsBefore);
        verify(mockHintExecutor, times(1)).submit(any(Runnable.class));
        verify(mockMutationExecutor, never()).submit(any(Runnable.class));
    }
}
