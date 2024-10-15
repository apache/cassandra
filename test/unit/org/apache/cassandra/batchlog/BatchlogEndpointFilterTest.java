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
package org.apache.cassandra.batchlog;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.GossipingPropertyFileSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class BatchlogEndpointFilterTest
{
    private static final String LOCAL = "local";
    private double oldBadness;

    @BeforeClass
    public static void initialiseServer()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void before()
    {
        oldBadness = DatabaseDescriptor.getDynamicBadnessThreshold();
        DatabaseDescriptor.setDynamicBadnessThreshold(0.1);
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
        StorageService.instance.unsafeInitialize();
    }

    @After
    public void after()
    {
        DatabaseDescriptor.setDynamicBadnessThreshold(oldBadness);
    }

    // Repeat all tests some more times since we're dealing with random stuff - i.e. increase the
    // chance to hit issues.
    private static final int repetitions = 100;
    private static final InetAddressAndPort[] INET_ADDRESSES = new InetAddressAndPort[0];

    private DynamicEndpointSnitch dsnitch;

    @Test
    public void shouldUseLocalRackIfPreferLocalParameter() throws UnknownHostException
    {
        DatabaseDescriptor.setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy.random_remote);
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                                                                          .put(LOCAL, InetAddressAndPort.getByName("0"))
                                                                          .put(LOCAL, InetAddressAndPort.getByName("00"))
                                                                          .put("1", InetAddressAndPort.getByName("1"))
                                                                          .put("1", InetAddressAndPort.getByName("11"))
                                                                          .put("2", InetAddressAndPort.getByName("2"))
                                                                          .put("2", InetAddressAndPort.getByName("22"))
                                                                          .build();
        Collection<InetAddressAndPort> result = filterBatchlogEndpointsRandomForTests(true, endpoints);
        assertThat(result.size()).isEqualTo(2);
        assertThat(result).containsAnyElementsOf(endpoints.get(LOCAL));
        assertThat(result).containsAnyElementsOf(Iterables.concat(endpoints.get("1"), endpoints.get("2")));
    }

    @Test
    public void shouldUseLocalRackIfPreferLocalStrategy() throws UnknownHostException
    {
        DatabaseDescriptor.setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy.prefer_local);
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                .put(LOCAL, InetAddressAndPort.getByName("0"))
                .put(LOCAL, InetAddressAndPort.getByName("00"))
                .put("1", InetAddressAndPort.getByName("1"))
                .put("1", InetAddressAndPort.getByName("11"))
                .put("2", InetAddressAndPort.getByName("2"))
                .put("2", InetAddressAndPort.getByName("22"))
                .build();

        Collection<InetAddressAndPort> result = filterBatchlogEndpointsRandomForTests(false, endpoints);
        assertThat(result.size()).isEqualTo(2);
        assertThat(result).containsAnyElementsOf(endpoints.get(LOCAL));
        assertThat(result).containsAnyElementsOf(Iterables.concat(endpoints.get("1"), endpoints.get("2")));
    }

    @Test
    public void shouldSelect2HostsFromNonLocalRacks() throws UnknownHostException
    {
        DatabaseDescriptor.setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy.random_remote);
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                .put(LOCAL, InetAddressAndPort.getByName("0"))
                .put(LOCAL, InetAddressAndPort.getByName("00"))
                .put("1", InetAddressAndPort.getByName("1"))
                .put("1", InetAddressAndPort.getByName("11"))
                .put("2", InetAddressAndPort.getByName("2"))
                .put("2", InetAddressAndPort.getByName("22"))
                .build();
        Collection<InetAddressAndPort> result = filterBatchlogEndpointsRandomForTests(false, endpoints);
        assertThat(result.size()).isEqualTo(2);
        assertTrue(result.contains(InetAddressAndPort.getByName("11")));
        assertTrue(result.contains(InetAddressAndPort.getByName("22")));
    }

    @Test
    public void shouldSelectLastHostsFromLastNonLocalRacks() throws UnknownHostException
    {
        DatabaseDescriptor.setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy.random_remote);
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                                                         .put(LOCAL, InetAddressAndPort.getByName("00"))
                                                         .put("1", InetAddressAndPort.getByName("11"))
                                                         .put("2", InetAddressAndPort.getByName("2"))
                                                         .put("2", InetAddressAndPort.getByName("22"))
                                                         .put("3", InetAddressAndPort.getByName("3"))
                                                         .put("3", InetAddressAndPort.getByName("33"))
                                                         .build();
        

        Collection<InetAddressAndPort> result = filterBatchlogEndpointsRandomForTests(false, endpoints);
        assertThat(result.size()).isEqualTo(2);

        // result should be the last replicas of the last two racks
        // (Collections.shuffle has been replaced with Collections.reverse for testing)
        assertTrue(result.contains(InetAddressAndPort.getByName("22")));
        assertTrue(result.contains(InetAddressAndPort.getByName("33")));
    }

    @Test
    public void shouldSelectHostFromLocal() throws UnknownHostException
    {
        DatabaseDescriptor.setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy.random_remote);
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                .put(LOCAL, InetAddressAndPort.getByName("0"))
                .put(LOCAL, InetAddressAndPort.getByName("00"))
                .put("1", InetAddressAndPort.getByName("1"))
                .build();

        Collection<InetAddressAndPort> result = filterBatchlogEndpointsRandomForTests(false, endpoints);
        assertThat(result.size()).isEqualTo(2);
        assertTrue(result.contains(InetAddressAndPort.getByName("1")));
        assertTrue(result.contains(InetAddressAndPort.getByName("0")));
    }

    @Test
    public void shouldReturnPassedEndpointForSingleNodeDC() throws UnknownHostException
    {
        DatabaseDescriptor.setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy.random_remote);
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                .put(LOCAL, InetAddressAndPort.getByName("0"))
                .build();

        Collection<InetAddressAndPort> result = filterBatchlogEndpointsRandomForTests(false, endpoints);
        assertThat(result.size()).isEqualTo(1);
        assertTrue(result.contains(InetAddressAndPort.getByName("0")));
    }

    @Test
    public void shouldSelectTwoRandomHostsFromSingleOtherRack() throws UnknownHostException
    {
        DatabaseDescriptor.setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy.random_remote);
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                .put(LOCAL, InetAddressAndPort.getByName("0"))
                .put(LOCAL, InetAddressAndPort.getByName("00"))
                .put("1", InetAddressAndPort.getByName("1"))
                .put("1", InetAddressAndPort.getByName("11"))
                .put("1", InetAddressAndPort.getByName("111"))
                .build();

        Collection<InetAddressAndPort> result = filterBatchlogEndpointsRandomForTests(false, endpoints);
        // result should be the last two non-local replicas
        // (Collections.shuffle has been replaced with Collections.reverse for testing)
        assertThat(result.size(), is(2));
        assertTrue(result.contains(InetAddressAndPort.getByName("11")));
        assertTrue(result.contains(InetAddressAndPort.getByName("111")));
    }

    @Test
    public void shouldSelectTwoRandomHostsFromSingleRack() throws UnknownHostException
    {
        DatabaseDescriptor.setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy.random_remote);
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                .put(LOCAL, InetAddressAndPort.getByName("1"))
                .put(LOCAL, InetAddressAndPort.getByName("11"))
                .put(LOCAL, InetAddressAndPort.getByName("111"))
                .put(LOCAL, InetAddressAndPort.getByName("1111"))
                .build();

        Collection<InetAddressAndPort> result = filterBatchlogEndpointsRandomForTests(false, endpoints);
        // result should be the last two non-local replicas
        // (Collections.shuffle has been replaced with Collections.reverse for testing)
        assertThat(result.size(), is(2));
        assertTrue(result.contains(InetAddressAndPort.getByName("111")));
        assertTrue(result.contains(InetAddressAndPort.getByName("1111")));
    }

    @Test
    public void shouldSelectOnlyTwoHostsEvenIfLocal() throws UnknownHostException
    {
        DatabaseDescriptor.setBatchlogEndpointStrategy(Config.BatchlogEndpointStrategy.random_remote);
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                                                         .put(LOCAL, InetAddressAndPort.getByName("1"))
                                                         .put(LOCAL, InetAddressAndPort.getByName("11"))
                                                         .build();

        Collection<InetAddressAndPort> result = filterBatchlogEndpointsRandomForTests(false, endpoints);
        assertThat(result.size()).isEqualTo(2);
        assertTrue(result.contains(InetAddressAndPort.getByName("1")));
        assertTrue(result.contains(InetAddressAndPort.getByName("11")));
    }

    private Collection<InetAddressAndPort> filterBatchlogEndpointsRandomForTests(boolean preferLocalRack, Multimap<String, InetAddressAndPort> endpoints)
    {
        return ReplicaPlans.filterBatchlogEndpointsRandom(preferLocalRack, LOCAL, endpoints,
                // Reverse instead of shuffle
                Collections::reverse,
                // Always alive
                (addr) -> true,
                // Always pick the last
                (size) -> size - 1);
    }

    private Collection<InetAddressAndPort> filterBatchlogEndpointsForTests(Multimap<String, InetAddressAndPort> endpoints)
    {
        return DatabaseDescriptor.getBatchlogEndpointStrategy().useDynamicSnitchScores ?
                filterBatchlogEndpointsDynamicForTests(endpoints) :
                filterBatchlogEndpointsRandomForTests(false, endpoints);
    }

    private Collection<InetAddressAndPort> filterBatchlogEndpointsDynamicForTests(Multimap<String, InetAddressAndPort> endpoints) {
        return ReplicaPlans.filterBatchlogEndpointsDynamic(false, LOCAL, endpoints, x -> true);
    }


    @Test
    public void shouldUseCoordinatorForSingleNodeDC()
    {
        withConfigs(Stream.of(
                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Collections.singletonList(LOCAL),
                        1),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Collections.singletonList(LOCAL),
                        1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Collections.singletonList(LOCAL),
                        1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Collections.singletonList(LOCAL),
                        1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Collections.singletonList(LOCAL),
                        1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Collections.singletonList(LOCAL),
                        1)
        ), this::shouldUseCoordinatorForSingleNodeDC);
    }

    private void shouldUseCoordinatorForSingleNodeDC(Multimap<String, InetAddressAndPort> endpoints)
    {
        Collection<InetAddressAndPort> result = filterBatchlogEndpointsForTests(endpoints);
        assertThat(result.size(), is(1));
        assertThat(result, hasItem(endpointAddress(0, 0)));
    }

    @Test
    public void shouldUseCoordinatorAndTheOtherForTwoNodesInOneRack()
    {
        withConfigs(Stream.of(
                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Collections.singletonList(LOCAL),
                        2),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Collections.singletonList(LOCAL),
                        2),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Collections.singletonList(LOCAL),
                        2),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Collections.singletonList(LOCAL),
                        2),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Collections.singletonList(LOCAL),
                        2),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Collections.singletonList(LOCAL),
                        2)
        ), this::shouldUseCoordinatorAndTheOtherForTwoNodesInOneRack);
    }

    private void shouldUseCoordinatorAndTheOtherForTwoNodesInOneRack(Multimap<String, InetAddressAndPort> endpoints)
    {
        Collection<InetAddressAndPort> result = filterBatchlogEndpointsForTests(endpoints);
        assertThat(result.size(), is(2));
        assertThat(new HashSet<>(result).size(), is(2));
        assertThat(result, hasItem(endpointAddress(0, 0)));
        assertThat(result, hasItem(endpointAddress(0, 1)));
    }

    @Test
    public void shouldUseCoordinatorAndTheOtherForTwoNodesInTwoRacks()
    {
        withConfigs(Stream.of(
                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Arrays.asList(LOCAL, "r1"),
                        1, 1),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Arrays.asList(LOCAL, "r1"),
                        1, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "r1"),
                        1, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Arrays.asList(LOCAL, "r1"),
                        1, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "r1"),
                        1, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Arrays.asList(LOCAL, "r1"),
                        1, 1)
        ), this::shouldUseCoordinatorAndTheOtherForTwoNodesInTwoRacks);
    }

    private void shouldUseCoordinatorAndTheOtherForTwoNodesInTwoRacks(Multimap<String, InetAddressAndPort> endpoints)
    {
        Collection<InetAddressAndPort> result = filterBatchlogEndpointsForTests(endpoints);
        assertThat(result.size(), is(2));
        assertThat(new HashSet<>(result).size(), is(2));
        assertThat(result, hasItem(endpointAddress(0, 0)));
        assertThat(result, hasItem(endpointAddress(1, 0)));
    }

    @Test
    public void shouldSelectOneNodeFromLocalRackAndOneNodeFromTheOtherRack()
    {
        withConfigs(Stream.of(
                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Arrays.asList(LOCAL, "1"),
                        2, 1),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Arrays.asList(LOCAL, "1"),
                        2, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "r1"),
                        2, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Arrays.asList(LOCAL, "r1"),
                        2, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "r1"),
                        2, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Arrays.asList(LOCAL, "r1"),
                        2, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "r1"),
                        1, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Arrays.asList(LOCAL, "r1"),
                        1, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "r1"),
                        1, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Arrays.asList(LOCAL, "r1"),
                        1, 1)
        ), this::shouldSelectOneNodeFromLocalRackAndOneNodeFromTheOtherRack);
    }

    private void shouldSelectOneNodeFromLocalRackAndOneNodeFromTheOtherRack(Multimap<String, InetAddressAndPort> endpoints)
    {
        for (int i = 0; i < repetitions; i++)
        {
            Collection<InetAddressAndPort> result = filterBatchlogEndpointsForTests(endpoints);
            assertThat(result.size(), is(2));
            assertThat(new HashSet<>(result).size(), is(2));
            assertThat(result, hasItem(endpointAddress(1, 0)));
            assertThat(result, either(hasItem(endpointAddress(0, 0)))
                    .or(hasItem(endpointAddress(0, 1))));
        }
    }

    @Test
    public void shouldReturnNoBatchlogEnpointsIfAllAreUnavailable()
    {
        withConfigs(Stream.of(
                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Collections.singletonList(LOCAL),
                        3),

                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Collections.singletonList(LOCAL),
                        15),

                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),

                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15)
        ), this::shouldReturnNoBatchlogEnpointsIfAllAreUnavailable);
    }

    private void shouldReturnNoBatchlogEnpointsIfAllAreUnavailable(Multimap<String, InetAddressAndPort> endpoints)
    {
        Predicate<InetAddressAndPort> isAlive = x -> x.equals(endpointAddress(0, 0));
        for (int i = 0; i < repetitions; i++)
        {
            Collection<InetAddressAndPort> result = DatabaseDescriptor.getBatchlogEndpointStrategy().useDynamicSnitchScores ?
                    ReplicaPlans.filterBatchlogEndpointsDynamic(false, LOCAL, endpoints, isAlive) :
                    ReplicaPlans.filterBatchlogEndpointsRandom(false, LOCAL, endpoints, Collections::reverse, isAlive, (size) -> size - 1);
            Assert.assertEquals(0, result.size());
        }
    }

    @Test
    public void shouldNotFailIfThereAreAtLeastTwoLiveNodesBesideCoordinator()
    {
        withConfigs(Stream.of(
                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Collections.singletonList(LOCAL),
                        3),

                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Collections.singletonList(LOCAL),
                        15),

                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),

                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15)
        ), this::shouldNotFailIfThereAreAtLeastTwoLiveNodesBesideCoordinator);
    }

    private void shouldNotFailIfThereAreAtLeastTwoLiveNodesBesideCoordinator(Multimap<String, InetAddressAndPort> endpoints)
    {
        Predicate<InetAddressAndPort> isAlive = x -> nodeInRack(x) >= endpoints.get(LOCAL).size() - 2;
        for (int i = 0; i < repetitions; i++)
        {
            if (DatabaseDescriptor.getBatchlogEndpointStrategy().useDynamicSnitchScores) {
                ReplicaPlans.filterBatchlogEndpointsDynamic(false, LOCAL, endpoints, isAlive);
            } else {
                ReplicaPlans.filterBatchlogEndpointsRandom(false, LOCAL, endpoints, Collections::reverse, isAlive, (size) -> size - 1);
            }
        }
    }

    @Test
    public void shouldNotFailIfThereAreAtLeastTwoLiveNodesIncludingCoordinator()
    {
        withConfigs(Stream.of(
                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Collections.singletonList(LOCAL),
                        3),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Collections.singletonList(LOCAL),
                        3),

                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Collections.singletonList(LOCAL),
                        15),

                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Arrays.asList(LOCAL, "r1"),
                        15, 15),

                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.prefer_local, true,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, false,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, false,
                        Arrays.asList(LOCAL, "r1", "r2"),
                        15, 15, 15)
        ), this::shouldNotFailIfThereAreAtLeastTwoLiveNodesIncludingCoordinator);
    }

    private void shouldNotFailIfThereAreAtLeastTwoLiveNodesIncludingCoordinator(Multimap<String, InetAddressAndPort> endpoints)
    {
        Predicate<InetAddressAndPort> isAlive = x -> nodeInRack(x) <= 1;
        for (int i = 0; i < repetitions; i++)
        {
            if (DatabaseDescriptor.getBatchlogEndpointStrategy().useDynamicSnitchScores) {
                ReplicaPlans.filterBatchlogEndpointsDynamic(false, LOCAL, endpoints, isAlive);
            } else {
                ReplicaPlans.filterBatchlogEndpointsRandom(false, LOCAL, endpoints, Collections::reverse, isAlive, (size) -> size - 1);
            }
        }
    }

    @Test
    public void shouldSelectTwoHostsFromNonLocalRacks()
    {
        withConfigs(Stream.of(
                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Arrays.asList(LOCAL, "1", "2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Arrays.asList(LOCAL, "1", "2"),
                        15, 1, 1),
                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Arrays.asList(LOCAL, "1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.random_remote, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "1", "2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "1", "2"),
                        15, 1, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Arrays.asList(LOCAL, "1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                        Collections.singletonList(LOCAL),
                        15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "1", "2"),
                        15, 15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "1", "2"),
                        15, 1, 1),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Arrays.asList(LOCAL, "1"),
                        15, 15),
                () -> configure(Config.BatchlogEndpointStrategy.dynamic, true,
                        Collections.singletonList(LOCAL),
                        15)
        ), this::assertTwoEndpointsWithoutCoordinator);
    }

    private void assertTwoEndpointsWithoutCoordinator(Multimap<String, InetAddressAndPort> endpoints)
    {
        for (int i = 0; i < repetitions; i++)
        {
            Collection<InetAddressAndPort> result = filterBatchlogEndpointsDynamicForTests(endpoints);
            // result should be the last two non-local replicas
            // (Collections.shuffle has been replaced with Collections.reverse for testing)
            assertThat(result.size(), is(2));
            assertThat(new HashSet<>(result).size(), is(2));
            assertThat(result, not(hasItems(endpoints.get(LOCAL).toArray(INET_ADDRESSES))));
        }
    }

    /**
     * Test with {@link Config.BatchlogEndpointStrategy#dynamic}.
     */
    @Test
    public void shouldSelectTwoFastestHostsFromSingleLocalRackWithDynamicSnitch()
    {
        for (int i = 0; i < repetitions; i++)
        {
            InetAddressAndPort host1 = endpointAddress(0, 1);
            InetAddressAndPort host2 = endpointAddress(0, 2);
            InetAddressAndPort host3 = endpointAddress(0, 3);
            List<InetAddressAndPort> hosts = Arrays.asList(host1, host2, host3);

            Multimap<String, InetAddressAndPort> endpoints = configure(Config.BatchlogEndpointStrategy.dynamic, true,
                    Collections.singletonList(LOCAL),
                    20);

            // ascending
            setScores(endpoints, hosts, 10, 12, 14);
            List<InetAddressAndPort> order = Arrays.asList(host1, host2, host3);
            assertEquals(order, ReplicaPlans.sortByProximity(Arrays.asList(host3, host1, host2)));

            Collection<InetAddressAndPort> result = filterBatchlogEndpointsDynamicForTests(endpoints);
            assertThat(result.size(), is(2));
            assertThat(result, hasItem(host1));
            assertThat(result, hasItem(host2));

            // descending
            setScores(endpoints, hosts, 50, 9, 1);
            order = Arrays.asList(host3, host2, host1);
            assertEquals(order, ReplicaPlans.sortByProximity(Arrays.asList(host1, host2, host3)));
            result = filterBatchlogEndpointsDynamicForTests(endpoints);
            assertThat(result.size(), is(2));
            assertThat(result, hasItem(host2));
            assertThat(result, hasItem(host3));
        }
    }

    /**
     * Test with {@link Config.BatchlogEndpointStrategy#dynamic}.
     */
    @Test
    public void shouldSelectOneFastestHostsFromNonLocalRackWithDynamicSnitch()
    {
        for (int i = 0; i < repetitions; i++)
        {
            // for each rack, get last host (only in test), then sort all endpoints from each rack by scores
            Multimap<String, InetAddressAndPort> endpoints = configure(Config.BatchlogEndpointStrategy.dynamic, true,
                    Arrays.asList(LOCAL, "r1", "r2"),
                    15, 15, 15);
            InetAddressAndPort r0h1 = endpointAddress(0, 1);
            InetAddressAndPort r1h1 = endpointAddress(1, 0);
            InetAddressAndPort r1h2 = endpointAddress(1, 1);
            InetAddressAndPort r2h1 = endpointAddress(2, 0);
            InetAddressAndPort r2h2 = endpointAddress(2, 1);
            List<InetAddressAndPort> hosts = Arrays.asList(r0h1, r1h1, r1h2, r2h1, r2h2);

            // ascending
            setScores(endpoints, hosts, 11, 6/* r1h1 */, 12, 5/* r2h1 */, 10);
            Collection<InetAddressAndPort> result = filterBatchlogEndpointsDynamicForTests(endpoints);
            assertThat(result.size(), is(2));
            assertThat(result, hasItem(r1h1));
            assertThat(result, hasItem(r2h1));

            // descending
            setScores(endpoints, hosts, 5/* r0h1 */, 20, 5, 0/* r2h1 */, 15);
            result = filterBatchlogEndpointsDynamicForTests(endpoints);
            assertThat(result.size(), is(2));
            assertThat(result, hasItem(r0h1));
            assertThat(result, hasItem(r2h1));
        }
    }

    /**
     * Test with {@link Config.BatchlogEndpointStrategy#dynamic_remote}.
     */
    @Test
    public void shouldSelectTwoFastestHostsFromSingleLocalRackWithDynamicSnitchRemote()
    {
        for (int i = 0; i < repetitions; i++)
        {
            InetAddressAndPort host1 = endpointAddress(0, 1);
            InetAddressAndPort host2 = endpointAddress(0, 2);
            InetAddressAndPort host3 = endpointAddress(0, 3);
            List<InetAddressAndPort> hosts = Arrays.asList(host1, host2, host3);

            Multimap<String, InetAddressAndPort> endpoints = configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                    Collections.singletonList(LOCAL),
                    20);

            // ascending
            setScores(endpoints, hosts,
                    10, 12, 14);
            List<InetAddressAndPort> order = Arrays.asList(host1, host2, host3);
            assertEquals(order, ReplicaPlans.sortByProximity(Arrays.asList(host3, host1, host2)));

            Collection<InetAddressAndPort> result = filterBatchlogEndpointsDynamicForTests(endpoints);
            assertThat(result.size(), is(2));
            assertThat(result, hasItem(host1));
            assertThat(result, hasItem(host2));

            // descending
            setScores(endpoints, hosts,
                    50, 9, 1);
            order = Arrays.asList(host3, host2, host1);
            assertEquals(order, ReplicaPlans.sortByProximity(Arrays.asList(host1, host2, host3)));

            result = filterBatchlogEndpointsDynamicForTests(endpoints);
            assertThat(result.size(), is(2));
            assertThat(result, hasItem(host2));
            assertThat(result, hasItem(host3));
        }
    }

    /**
     * Test with {@link Config.BatchlogEndpointStrategy#dynamic_remote}.
     */
    @Test
    public void shouldSelectOneFastestHostsFromNonLocalRackWithDynamicSnitchRemote()
    {
        for (int i = 0; i < repetitions; i++)
        {
            // for each rack, get last host (only in test), then sort all endpoints from each rack by scores
            Multimap<String, InetAddressAndPort> endpoints = configure(Config.BatchlogEndpointStrategy.dynamic_remote, true,
                    Arrays.asList(LOCAL, "r1", "r2"),
                    15, 15, 15);
            InetAddressAndPort r0h1 = endpointAddress(0, 1);
            InetAddressAndPort r1h1 = endpointAddress(1, 0);
            InetAddressAndPort r1h2 = endpointAddress(1, 1);
            InetAddressAndPort r2h1 = endpointAddress(2, 0);
            InetAddressAndPort r2h2 = endpointAddress(2, 1);
            List<InetAddressAndPort> hosts = Arrays.asList(r0h1, r1h1, r1h2, r2h1, r2h2);

            // ascending
            setScores(endpoints, hosts,
                    1,
                    10, 12,
                    5, 10);

            Collection<InetAddressAndPort> result = filterBatchlogEndpointsDynamicForTests(endpoints);
            filterBatchlogEndpointsDynamicForTests(endpoints);
            assertThat(result.size(), is(2));
            assertThat(result, hasItem(r1h1));
            assertThat(result, hasItem(r2h1));

            // descending
            setScores(endpoints, hosts,
                    1, // rack 0
                    20, 5, // rack 1
                    0, 15); // rack 2
            result = filterBatchlogEndpointsDynamicForTests(endpoints);
            assertThat(result.size(), is(2));
            assertThat(result, hasItem(r1h2));
            assertThat(result, hasItem(r2h1));
        }
    }

    private void setScores(Multimap<String, InetAddressAndPort> endpoints,
                           List<InetAddressAndPort> hosts,
                           Integer... scores)
    {
        int maxScore = 0;

        // set the requested scores for the requested hosts
        for (int round = 0; round < 50; round++)
        {
            for (int i = 0; i < hosts.size(); i++)
            {
                dsnitch.receiveTiming(hosts.get(i), scores[i], MILLISECONDS);
                maxScore = Math.max(maxScore, scores[i]);
            }
        }

        // set some random (higher) scores for unrequested hosts
        for (InetAddressAndPort ep : endpoints.values())
        {
            if (hosts.contains(ep))
                continue;
            for (int r = 0; r < 1; r++)
                dsnitch.receiveTiming(ep, maxScore + ThreadLocalRandom.current().nextInt(100) + 1, MILLISECONDS);
        }

        dsnitch.updateScores();
    }

    private int nodeInRack(InetAddressAndPort input)
    {
        return input.addressBytes[3];
    }

    private static InetAddressAndPort endpointAddress(int rack, int nodeInRack)
    {
        if (rack == 0 && nodeInRack == 0)
            return FBUtilities.getBroadcastAddressAndPort();

        try
        {
            return InetAddressAndPort.getByAddress(new byte[]{ 0, 0, (byte) rack, (byte) nodeInRack });
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private Multimap<String, InetAddressAndPort> configure(Config.BatchlogEndpointStrategy batchlogEndpointStrategy,
                                                           boolean dynamicSnitch,
                                                           List<String> racks,
                                                           int... nodesPerRack)
    {
        // if any of the three assertions fires, your test is busted
        assert !racks.isEmpty();
        assert racks.size() <= 10;
        assert racks.size() == nodesPerRack.length;

        ImmutableMultimap.Builder<String, InetAddressAndPort> builder = ImmutableMultimap.builder();
        for (int r = 0; r < racks.size(); r++)
        {
            String rack = racks.get(r);
            for (int n = 0; n < nodesPerRack[r]; n++)
                builder.put(rack, endpointAddress(r, n));
        }

        ImmutableMultimap<String, InetAddressAndPort> endpoints = builder.build();

        reconfigure(batchlogEndpointStrategy, dynamicSnitch, endpoints);

        return endpoints;
    }

    private void reconfigure(Config.BatchlogEndpointStrategy batchlogEndpointStrategy,
                             boolean dynamicSnitch,
                             Multimap<String, InetAddressAndPort> endpoints)
    {
        DatabaseDescriptor.setBatchlogEndpointStrategy(batchlogEndpointStrategy);

        if (DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch)
            ((DynamicEndpointSnitch) DatabaseDescriptor.getEndpointSnitch()).close();

        Multimap<InetAddressAndPort, String> endpointRacks = Multimaps.invertFrom(endpoints, ArrayListMultimap.create());
        GossipingPropertyFileSnitch gpfs = new GossipingPropertyFileSnitch()
        {
            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return "dc1";
            }

            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                return endpointRacks.get(endpoint).iterator().next();
            }
        };
        IEndpointSnitch snitch;
        if (dynamicSnitch)
            snitch = dsnitch = new DynamicEndpointSnitch(gpfs, String.valueOf(gpfs.hashCode()));
        else
        {
            dsnitch = null;
            snitch = gpfs;
        }

        DatabaseDescriptor.setDynamicBadnessThreshold(0);
        DatabaseDescriptor.setEndpointSnitch(snitch);

        DatabaseDescriptor.setBatchlogEndpointStrategy(batchlogEndpointStrategy);
    }

    private void withConfigs(Stream<Supplier<Multimap<String, InetAddressAndPort>>> supplierStream,
                             Consumer<Multimap<String, InetAddressAndPort>> testFunction)
    {
        supplierStream.map(Supplier::get)
                .forEach(endpoints -> {
                    try
                    {
                        testFunction.accept(endpoints);
                    }
                    catch (AssertionError e)
                    {
                        throw new AssertionError(configToString(endpoints), e);
                    }
                });
    }

    private String configToString(Multimap<String, InetAddressAndPort> endpoints)
    {
        return "strategy:" + DatabaseDescriptor.getBatchlogEndpointStrategy()
                + " snitch:" + DatabaseDescriptor.getEndpointSnitch().getClass().getSimpleName()
                + " nodes-per-rack: " + endpoints.asMap().entrySet().stream()
                .map(e -> e.getKey() + '=' + e.getValue().size())
                .collect(Collectors.joining());
    }

}
