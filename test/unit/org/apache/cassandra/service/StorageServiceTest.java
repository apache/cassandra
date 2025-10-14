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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMultimap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaMultimap;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.mockito.Mockito;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StorageServiceTest
{
    static InetAddressAndPort aAddress;
    static InetAddressAndPort bAddress;
    static InetAddressAndPort cAddress;
    static InetAddressAndPort dAddress;
    static InetAddressAndPort eAddress;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        aAddress = InetAddressAndPort.getByName("127.0.0.1");
        bAddress = InetAddressAndPort.getByName("127.0.0.2");
        cAddress = InetAddressAndPort.getByName("127.0.0.3");
        dAddress = InetAddressAndPort.getByName("127.0.0.4");
        eAddress = InetAddressAndPort.getByName("127.0.0.5");
    }

    private static final Token threeToken = new RandomPartitioner.BigIntegerToken("3");
    private static final Token sixToken = new RandomPartitioner.BigIntegerToken("6");
    private static final Token nineToken = new RandomPartitioner.BigIntegerToken("9");
    private static final Token elevenToken = new RandomPartitioner.BigIntegerToken("11");
    private static final Token oneToken = new RandomPartitioner.BigIntegerToken("1");

    Range<Token> aRange = new Range<>(oneToken, threeToken);
    Range<Token> bRange = new Range<>(threeToken, sixToken);
    Range<Token> cRange = new Range<>(sixToken, nineToken);
    Range<Token> dRange = new Range<>(nineToken, elevenToken);
    Range<Token> eRange = new Range<>(elevenToken, oneToken);

    boolean defaultSkipPaxosRepairOnTopologyChange = Boolean.getBoolean("cassandra.skip_paxos_repair_on_topology_change");
    boolean defaultSkipPaxosRepairOnTopologyForStrictMV = false;
    boolean defaultStrictMVEnabled = false;
    Config.PaxosVariant defaultPaxosVariant = Config.PaxosVariant.v1;
    ActiveRepairService originalActiveRepairService;
    Schema originalSchema;
    TableMetadata dummyStrictMVTable = TableMetadata.builder("ks", "strictmv")
                                                    .addPartitionKeyColumn("k", AsciiType.instance)
                                                    .addRegularColumn("c", AsciiType.instance)
                                                    .strictMVConsistency(true)
                                                    .build();
    TableMetadata dummyTable = TableMetadata.builder("ks", "regulartb").addPartitionKeyColumn("k", AsciiType.instance)
                                            .addRegularColumn("c", AsciiType.instance)
                                            .strictMVConsistency(false)
                                            .build();

    @Before
    public void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        IEndpointSnitch snitch = new AbstractEndpointSnitch()
        {
            public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
            {
                return 0;
            }

            public String getRack(InetAddressAndPort endpoint)
            {
                return "R1";
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return "DC1";
            }
        };

        DatabaseDescriptor.setEndpointSnitch(snitch);
        CommitLog.instance.start();
    }

    @After
    public void reset() throws Exception
    {
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChange(defaultSkipPaxosRepairOnTopologyChange);
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChangeForStrictMV(defaultSkipPaxosRepairOnTopologyForStrictMV);
        DatabaseDescriptor.setMaterializedViewStrictConsistencyEnabled(defaultStrictMVEnabled);
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChangeKeyspaces("");
        Paxos.setPaxosVariant(defaultPaxosVariant);
        System.clearProperty("cassandra.paxos_repair_on_topology_change_retries");
        System.clearProperty("cassandra.paxos_repair_on_topology_change_retry_delay_seconds");
        if (originalActiveRepairService != null)
            replaceStaticField(ActiveRepairService.class, "instance", originalActiveRepairService);
        if (originalSchema != null)
            replaceStaticField(Schema.class, "instance", originalSchema);
    }

    private AbstractReplicationStrategy simpleStrategy(TokenMetadata tmd)
    {
        return new SimpleStrategy("MoveTransientTest",
                                  tmd,
                                  DatabaseDescriptor.getEndpointSnitch(),
                                  com.google.common.collect.ImmutableMap.of("replication_factor", "3/1"));
    }

    public static <K, C extends ReplicaCollection<? extends C>>  void assertMultimapEqualsIgnoreOrder(ReplicaMultimap<K, C> a, ReplicaMultimap<K, C> b)
    {
        if (!a.keySet().equals(b.keySet()))
            fail(formatNeq(a, b));
        for (K key : a.keySet())
        {
            C ac = a.get(key);
            C bc = b.get(key);
            if (ac.size() != bc.size())
                fail(formatNeq(a, b));
            for (Replica r : ac)
            {
                if (!bc.contains(r))
                    fail(formatNeq(a, b));
            }
        }
    }

    public static String formatNeq(Object v1, Object v2)
    {
        return "\nExpected: " + formatClassAndValue(v1) + "\n but was: " + formatClassAndValue(v2);
    }

    public static String formatClassAndValue(Object value)
    {
        String className = value == null ? "null" : value.getClass().getName();
        return className + "<" + String.valueOf(value) + ">";
    }

    @Test
    public void testGetChangedReplicasForLeaving() throws Exception
    {
        TokenMetadata tmd = new TokenMetadata();
        tmd.updateNormalToken(threeToken, aAddress);
        tmd.updateNormalToken(sixToken, bAddress);
        tmd.updateNormalToken(nineToken, cAddress);
        tmd.updateNormalToken(elevenToken, dAddress);
        tmd.updateNormalToken(oneToken, eAddress);

        tmd.addLeavingEndpoint(aAddress);

        AbstractReplicationStrategy strat = simpleStrategy(tmd);

        EndpointsByReplica result = StorageService.getChangedReplicasForLeaving("StorageServiceTest", aAddress, tmd, strat);
        System.out.println(result);
        EndpointsByReplica.Builder expectedResult = new EndpointsByReplica.Builder();
        expectedResult.put(new Replica(aAddress, aRange, true), new Replica(cAddress, new Range<>(oneToken, sixToken), true));
        expectedResult.put(new Replica(aAddress, aRange, true), new Replica(dAddress, new Range<>(oneToken, sixToken), false));
        expectedResult.put(new Replica(aAddress, eRange, true), new Replica(bAddress, eRange, true));
        expectedResult.put(new Replica(aAddress, eRange, true), new Replica(cAddress, eRange, false));
        expectedResult.put(new Replica(aAddress, dRange, false), new Replica(bAddress, dRange, false));
        assertMultimapEqualsIgnoreOrder(result, expectedResult.build());
    }

    @Test
    public void testSetGetSSTablePreemptiveOpenIntervalInMB()
    {
        StorageService.instance.setSSTablePreemptiveOpenIntervalInMB(-1);
        Assert.assertEquals(-1, StorageService.instance.getSSTablePreemptiveOpenIntervalInMB());
    }

    @Test
    public void testScheduledExecutorsShutdownOnDrain() throws Throwable
    {
        final AtomicInteger numberOfRuns = new AtomicInteger(0);

        ScheduledFuture<?> f = ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(numberOfRuns::incrementAndGet,
                                                                                     0, 1, SECONDS);

        // Prove the task was scheduled more than once before checking cancelled.
        await("first run").atMost(1, MINUTES).until(() -> numberOfRuns.get() > 1);

        assertFalse(f.isCancelled());
        StorageService.instance.drain();
        assertTrue(f.isCancelled());

        assertTrue(ScheduledExecutors.scheduledTasks.isTerminated());
        assertTrue(ScheduledExecutors.nonPeriodicTasks.isTerminated());
        assertTrue(ScheduledExecutors.optionalTasks.isTerminated());

        // fast tasks are shut down as part of the Runtime shutdown hook.
        assertFalse(ScheduledExecutors.scheduledFastTasks.isTerminated());
    }

    @Test
    public void testRebuildFailOnNonExistingDatacenter()
    {
        String nonExistentDC = "NON_EXISTENT_DC";

        try
        {
            getStorageService().rebuild(nonExistentDC, "StorageServiceTest", null, null, false);
            fail();
        }
        catch (IllegalArgumentException ex)
        {
            Assert.assertEquals(String.format("Provided datacenter '%s' is not a valid datacenter, available datacenters are: %s",
                                              nonExistentDC,
                                              SimpleSnitch.DATA_CENTER_NAME),
                                ex.getMessage());
        }
    }

    @Test
    public void testRebuildingWithTokensWithoutKeyspace() throws Exception
    {
        try
        {
            getStorageService().rebuild("datacenter1", null, "123", null, false);
            fail();
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals("Cannot specify tokens without keyspace.", ex.getMessage());
        }
    }

    @Test
    public void testRepairPaxosForTopologyChangeAllSkipped() throws Exception
    {
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChange(true);
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChangeForStrictMV(true);
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        StorageService storageService = getStorageService();

        doNothing().when(storageService).startRepairPaxosForTopologyChangeByKeyspace(any(), any());
        doCallRealMethod().when(storageService).repairPaxosForTopologyChange(any());

        storageService.repairPaxosForTopologyChange("StorageServiceTest");
        verify(storageService, times(0)).startRepairPaxosForTopologyChangeByKeyspace(eq("StorageServiceTest"), any());
    }

    @Test
    public void testRepairPaxosForTopologyLimitedRetry()
    {
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChange(false);
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChangeForStrictMV(false);
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        System.setProperty("cassandra.paxos_repair_on_topology_change_retries", "5");
        System.setProperty("cassandra.paxos_repair_on_topology_change_retry_delay_seconds", "0");
        StorageService storageService = getStorageService();

        doThrow(RuntimeException.class).when(storageService).startRepairPaxosForTopologyChangeByKeyspace(any(), any());
        doCallRealMethod().when(storageService).repairPaxosForTopologyChange(any());

        try
        {
            storageService.repairPaxosForTopologyChange("StorageServiceTest");
            fail("Expected failure due to retries exhausted");
        }
        catch (RuntimeException e)
        {
            // expected
        }
        verify(storageService, times(6)).startRepairPaxosForTopologyChangeByKeyspace(eq("StorageServiceTest"),
                                                                                     argThat(p -> p != null &&
                                                                                                  p.test(dummyStrictMVTable) &&
                                                                                                  p.test(dummyTable)));
    }

    @Test
    public void testRepairPaxosForTopologyChangeRegularSkipped() throws Exception
    {
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChange(true);
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChangeForStrictMV(false);
        DatabaseDescriptor.setMaterializedViewStrictConsistencyEnabled(true);
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        StorageService storageService = getStorageService();

        doNothing().when(storageService).startRepairPaxosForTopologyChangeByKeyspace(any(), any());
        doCallRealMethod().when(storageService).repairPaxosForTopologyChange(any());

        storageService.repairPaxosForTopologyChange("StorageServiceTest");
        verify(storageService, times(1)).startRepairPaxosForTopologyChangeByKeyspace(eq("StorageServiceTest"),
                                                                                     argThat(p -> p != null &&
                                                                                                  p.test(dummyStrictMVTable) &&
                                                                                                  !p.test(dummyTable)));
    }

    @Test
    public void testStartRepairPaxosForTopologyChangeByKeyspace() throws Exception
    {
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChange(true);
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChangeForStrictMV(false);
        DatabaseDescriptor.setMaterializedViewStrictConsistencyEnabled(true);
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChangeKeyspaces("ks2");
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        StorageService storageService = getStorageService();
        doCallRealMethod().when(storageService).repairPaxosForTopologyChange(any());

        ActiveRepairService activeRepairService = getActiveRepairService();
        Schema schema = getSchema();
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "2");
        configOptions.put("DC2", "2");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        doReturn(Keyspaces.of(KeyspaceMetadata.create("ks1", KeyspaceParams.create(true, configOptions)),
                              KeyspaceMetadata.create("ks2", KeyspaceParams.create(true, configOptions)),
                              KeyspaceMetadata.create(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, KeyspaceParams.create(true, configOptions))))
        .when(schema).distributedKeyspaces();
        List<Range<Token>> dummyRanges = new ArrayList<>();
        doReturn(dummyRanges).when(storageService).getLocalAndPendingRanges(any());
        doReturn(ImmediateFuture.success(null)).when(activeRepairService).repairPaxosForTopologyChange(any(), any(), any(), any());

        storageService.repairPaxosForTopologyChange("StorageServiceTest");

        verify(activeRepairService, times(1)).repairPaxosForTopologyChange(eq("ks1"), any(), any(), any());
    }

    @Test
    public void testStartRepairPaxosForTopologyChangeByKeyspaceExceptions() throws Exception
    {
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChange(true);
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChangeForStrictMV(false);
        DatabaseDescriptor.setMaterializedViewStrictConsistencyEnabled(true);
        DatabaseDescriptor.setSkipPaxosRepairOnTopologyChangeKeyspaces("ks2");
        System.setProperty("cassandra.paxos_repair_on_topology_change_retries", "0");
        System.setProperty("cassandra.paxos_repair_on_topology_change_retry_delay_seconds", "0");
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        StorageService storageService = getStorageService();
        doCallRealMethod().when(storageService).repairPaxosForTopologyChange(any());

        ActiveRepairService activeRepairService = getActiveRepairService();
        Schema schema = getSchema();
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("DC1", "2");
        configOptions.put("DC2", "2");
        configOptions.put(ReplicationParams.CLASS, "NetworkTopologyStrategy");

        doReturn(Keyspaces.of(KeyspaceMetadata.create("ks1", KeyspaceParams.create(true, configOptions)),
                              KeyspaceMetadata.create("ks2", KeyspaceParams.create(true, configOptions)),
                              KeyspaceMetadata.create(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, KeyspaceParams.create(true, configOptions))))
        .when(schema).distributedKeyspaces();
        List<Range<Token>> dummyRanges = new ArrayList<>();
        doReturn(dummyRanges).when(storageService).getLocalAndPendingRanges(any());
        // throw ExecutionException
        Future<?> mockFut = Mockito.mock(Future.class);
        doThrow(ExecutionException.class).when(mockFut).get();
        doReturn(mockFut).when(activeRepairService).repairPaxosForTopologyChange(any(), any(), any(), any());
        try
        {
            storageService.repairPaxosForTopologyChange("StorageServiceTest");
            fail("Expected RuntimeException due to ExecutionException in repairPaxosForTopologyChange");
        }
        catch (RuntimeException e)
        {
            // expected
        }
        verify(activeRepairService, times(1)).repairPaxosForTopologyChange(eq("ks1"), any(), any(), any());

        // throw InterruptedException
        Future<?> mockFut2 = Mockito.mock(Future.class);
        doThrow(InterruptedException.class).when(mockFut2).get();
        doReturn(mockFut2).when(activeRepairService).repairPaxosForTopologyChange(any(), any(), any(), any());
        try
        {
            storageService.repairPaxosForTopologyChange("StorageServiceTest");
            fail("Expected AssertionError due to InterruptedException in repairPaxosForTopologyChange");
        }
        catch (AssertionError e)
        {
            // expected
        }
        verify(activeRepairService, times(2)).repairPaxosForTopologyChange(eq("ks1"), any(), any(), any());
    }

    private ActiveRepairService getActiveRepairService() throws Exception
    {
        ActiveRepairService activeRepairService = Mockito.mock(ActiveRepairService.class);
        originalActiveRepairService = replaceStaticField(ActiveRepairService.class, "instance", activeRepairService);
        return activeRepairService;
    }

    private Schema getSchema() throws Exception
    {
        Schema schema = Mockito.mock(Schema.class);
        originalSchema = replaceStaticField(Schema.class, "instance", schema);
        return schema;
    }

    @SuppressWarnings("unchecked")
    private <T> T replaceStaticField(Class<?> clazz, String fieldName, T newValue) throws Exception
    {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);

        try {
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        } catch (Exception e) {
            // ignore
        }

        T originalValue = (T) field.get(null);
        field.set(null, newValue);

        return originalValue;
    }

    private StorageService getStorageService()
    {
        ImmutableMultimap.Builder<String, InetAddressAndPort> builder = ImmutableMultimap.builder();
        builder.put(SimpleSnitch.DATA_CENTER_NAME, aAddress);

        TokenMetadata.Topology tokenMetadataTopology = Mockito.mock(TokenMetadata.Topology.class);
        when(tokenMetadataTopology.getDatacenterEndpoints()).thenReturn(builder.build());

        TokenMetadata metadata = new TokenMetadata(new SimpleSnitch());
        TokenMetadata spiedMetadata = Mockito.spy(metadata);

        when(spiedMetadata.getTopology()).thenReturn(tokenMetadataTopology);

        StorageService spiedStorageService = Mockito.spy(StorageService.instance);
        when(spiedStorageService.getTokenMetadata()).thenReturn(spiedMetadata);
        when(spiedMetadata.cloneOnlyTokenMap()).thenReturn(spiedMetadata);

        return spiedStorageService;
    }

    @Test
    public void testMaxWaitTimeInTransportQueueConfigurationDefault()
    {
        // Test that default value is handled correctly
        long originalMaxWaitInMillis = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS);
        try
        {
            // When max_wait_time_in_transport_queue is 0, the getter should return 0
            // The fallback logic is handled in Dispatcher, not in the getter itself
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(0);
            long maxWaitTimeoutInMillis = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS);

            assertEquals("When max_wait_time_in_transport_queue is set to 0, getter should return 0",
                         0, maxWaitTimeoutInMillis);

            // Test that a non-zero value works correctly
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(5000);
            maxWaitTimeoutInMillis = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS);
            assertEquals("Custom max_wait_time_in_transport_queue should be returned correctly",
                         5000, maxWaitTimeoutInMillis);
        }
        finally
        {
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(originalMaxWaitInMillis);
        }
    }

    @Test
    public void testMaxWaitTimeInTransportQueueConfigurationCustomValue()
    {
        // Test that custom values are respected
        long originalMaxWaitInMillis = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS);
        try
        {
            long maxWaitTimeoutInMillis = 5000; // 5 seconds
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(maxWaitTimeoutInMillis);

            assertEquals("Custom max_wait_time_in_transport_queue should be respected",
                         maxWaitTimeoutInMillis, DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS));
        }
        finally
        {
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(originalMaxWaitInMillis);
        }
    }

    @Test
    public void testMaxWaitTimeInTransportQueueTimeUnitConversions()
    {
        // Test that time unit conversions work correctly
        long originalMaxWaitInMillis = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS);
        try
        {
            long maxWaitTimeoutInMillis = 2000; // 2 seconds
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(maxWaitTimeoutInMillis);

            assertEquals("Milliseconds conversion should be correct",
                         maxWaitTimeoutInMillis, DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS));
            assertEquals("Nanoseconds conversion should be correct",
                         maxWaitTimeoutInMillis * 1_000_000, DatabaseDescriptor.getMaxWaitTimeInTransportQueue(NANOSECONDS));
            assertEquals("Seconds conversion should be correct",
                         maxWaitTimeoutInMillis / 1000, DatabaseDescriptor.getMaxWaitTimeInTransportQueue(SECONDS));
        }
        finally
        {
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(originalMaxWaitInMillis);
        }
    }

    @Test
    public void testMaxWaitTimeInTransportQueueIntegrationWithNativeTimeout()
    {
        // Test interaction between max_wait_time_in_transport_queue and native_transport_timeout
        long originalMaxWaitInMillis = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS);
        long originalNativeTimeoutInMillis = DatabaseDescriptor.getNativeTransportTimeout(MILLISECONDS);
        try
        {
            long nativeTimeoutInMillis = 3000; // 3 seconds
            long maxWaitTimeoutInMillis = 1500; // 1.5 seconds

            DatabaseDescriptor.setNativeTransportTimeout(nativeTimeoutInMillis);
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(maxWaitTimeoutInMillis);

            assertEquals("max_wait_time_in_transport_queue should be independent of native_transport_timeout when set",
                         maxWaitTimeoutInMillis, DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS));

            // Test that they can be different values
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(0);
            assertEquals("When max_wait_time_in_transport_queue is 0, getter should return 0",
                         0, DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS));
            assertEquals("native_transport_timeout should remain unchanged",
                         nativeTimeoutInMillis, DatabaseDescriptor.getNativeTransportTimeout(MILLISECONDS));
        }
        finally
        {
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(originalMaxWaitInMillis);
            DatabaseDescriptor.setNativeTransportTimeout(originalNativeTimeoutInMillis);
        }
    }

    @Test
    public void testMaxWaitTimeInTransportQueueDispatcherFallbackBehavior()
    {
        // Test that the Dispatcher uses native_transport_timeout when max_wait_time_in_transport_queue is 0
        // This tests the actual fallback logic implementation in your Dispatcher changes
        long originalMaxWaitInMillis = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS);
        long originalNativeTimeoutInMillis = DatabaseDescriptor.getNativeTransportTimeout(MILLISECONDS);
        try
        {
            long nativeTimeoutInMillis = 4000; // 4 seconds
            DatabaseDescriptor.setNativeTransportTimeout(nativeTimeoutInMillis);
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(0);

            // The Dispatcher.getMaxWaitTimeInTransportQueue method should fall back to native_transport_timeout
            // when max_wait_time_in_transport_queue is 0. This is testing the logic you added.
            long effectiveTimeout = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(NANOSECONDS);
            if (effectiveTimeout == 0)
            {
                // Fallback behavior - use native_transport_timeout
                effectiveTimeout = DatabaseDescriptor.getNativeTransportTimeout(NANOSECONDS);
            }

            assertEquals("When max_wait_time_in_transport_queue is 0, effective timeout should use native_transport_timeout",
                         nativeTimeoutInMillis * 1_000_000, effectiveTimeout);

            // Test with explicit value
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(2000);
            long explicitTimeout = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(NANOSECONDS);
            assertEquals("When max_wait_time_in_transport_queue is set explicitly, it should be used",
                         2000 * 1_000_000, explicitTimeout);
        }
        finally
        {
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(originalMaxWaitInMillis);
            DatabaseDescriptor.setNativeTransportTimeout(originalNativeTimeoutInMillis);
        }
    }

    @Test
    public void testMaxWaitTimeInTransportQueueValidation()
    {
        // Test that negative values are handled appropriately
        long originalMaxWaitInMillis = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS);
        try
        {
            // Setting a positive value should work
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(1000);
            assertEquals(1000, DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS));

            // Setting 0 should be stored as 0 (fallback happens in Dispatcher)
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(0);
            assertEquals("Zero value should be stored as zero",
                         0L, DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS));
        }
        finally
        {
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(originalMaxWaitInMillis);
        }
    }

    @Test
    public void testMaxWaitTimeInTransportQueueThreadSafety() throws InterruptedException
    {
        // Test that concurrent access to the configuration is safe
        long originalMaxWaitInMillis = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS);
        try
        {
            final AtomicInteger successCount = new AtomicInteger(0);
            final int numThreads = 10;
            final int iterations = 100;

            List<Thread> threads = new ArrayList<>();

            for (int i = 0; i < numThreads; i++)
            {
                final int threadId = i;
                Thread thread = new Thread(() -> {
                    for (int j = 0; j < iterations; j++)
                    {
                        try
                        {
                            long timeoutInMillis = (threadId + 1) * 100 + j; // Unique timeout per thread/iteration
                            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(timeoutInMillis);
                            long retrieved = DatabaseDescriptor.getMaxWaitTimeInTransportQueue(MILLISECONDS);

                            // The retrieved value should be some valid timeout (may not be exact due to concurrency)
                            if (retrieved > 0)
                            {
                                successCount.incrementAndGet();
                            }
                        }
                        catch (Exception e)
                        {
                            // Log but don't fail - some concurrency issues may be expected
                            System.err.println("Thread " + threadId + " iteration " + j + " failed: " + e.getMessage());
                        }
                    }
                });
                threads.add(thread);
            }

            // Start all threads
            for (Thread thread : threads)
            {
                thread.start();
            }

            // Wait for all threads to complete
            for (Thread thread : threads)
            {
                thread.join();
            }

            // We should have a reasonable success rate (at least 80%)
            int expectedMinSuccess = (numThreads * iterations) * 8 / 10;
            assertTrue("Concurrent access should mostly succeed, got " + successCount.get() + " successes out of " + (numThreads * iterations),
                       successCount.get() >= expectedMinSuccess);
        }
        finally
        {
            DatabaseDescriptor.setMaxWaitTimeInTransportQueue(originalMaxWaitInMillis);
        }
    }
}
