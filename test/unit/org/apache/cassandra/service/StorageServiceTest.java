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
        .when(schema).getNonLocalStrategyKeyspaces();
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
        .when(schema).getNonLocalStrategyKeyspaces();
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
}
