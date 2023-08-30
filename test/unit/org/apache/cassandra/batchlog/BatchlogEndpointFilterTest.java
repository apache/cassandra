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
import java.util.Collection;
import java.util.Collections;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlans;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BatchlogEndpointFilterTest
{
    private static final String LOCAL = "local";

    @BeforeClass
    public static void initialiseServer()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void shouldSelect2HostsFromNonLocalRacks() throws UnknownHostException
    {
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                .put(LOCAL, InetAddressAndPort.getByName("0"))
                .put(LOCAL, InetAddressAndPort.getByName("00"))
                .put("1", InetAddressAndPort.getByName("1"))
                .put("1", InetAddressAndPort.getByName("11"))
                .put("2", InetAddressAndPort.getByName("2"))
                .put("2", InetAddressAndPort.getByName("22"))
                .build();
        Collection<InetAddressAndPort> result = filterBatchlogEndpoints(endpoints);
        assertThat(result.size(), is(2));
        assertTrue(result.contains(InetAddressAndPort.getByName("11")));
        assertTrue(result.contains(InetAddressAndPort.getByName("22")));
    }

    @Test
    public void shouldSelectLastHostsFromLastNonLocalRacks() throws UnknownHostException
    {
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                                                         .put(LOCAL, InetAddressAndPort.getByName("00"))
                                                         .put("1", InetAddressAndPort.getByName("11"))
                                                         .put("2", InetAddressAndPort.getByName("2"))
                                                         .put("2", InetAddressAndPort.getByName("22"))
                                                         .put("3", InetAddressAndPort.getByName("3"))
                                                         .put("3", InetAddressAndPort.getByName("33"))
                                                         .build();
        
        Collection<InetAddressAndPort> result = filterBatchlogEndpoints(endpoints);
        assertThat(result.size(), is(2));

        // result should be the last replicas of the last two racks
        // (Collections.shuffle has been replaced with Collections.reverse for testing)
        assertTrue(result.contains(InetAddressAndPort.getByName("22")));
        assertTrue(result.contains(InetAddressAndPort.getByName("33")));
    }

    @Test
    public void shouldSelectHostFromLocal() throws UnknownHostException
    {
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                .put(LOCAL, InetAddressAndPort.getByName("0"))
                .put(LOCAL, InetAddressAndPort.getByName("00"))
                .put("1", InetAddressAndPort.getByName("1"))
                .build();
        Collection<InetAddressAndPort> result = filterBatchlogEndpoints(endpoints);
        assertThat(result.size(), is(2));
        assertTrue(result.contains(InetAddressAndPort.getByName("1")));
        assertTrue(result.contains(InetAddressAndPort.getByName("0")));
    }

    @Test
    public void shouldReturnPassedEndpointForSingleNodeDC() throws UnknownHostException
    {
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                .put(LOCAL, InetAddressAndPort.getByName("0"))
                .build();
        Collection<InetAddressAndPort> result = filterBatchlogEndpoints(endpoints);
        assertThat(result.size(), is(1));
        assertTrue(result.contains(InetAddressAndPort.getByName("0")));
    }

    @Test
    public void shouldSelectTwoRandomHostsFromSingleOtherRack() throws UnknownHostException
    {
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                .put(LOCAL, InetAddressAndPort.getByName("0"))
                .put(LOCAL, InetAddressAndPort.getByName("00"))
                .put("1", InetAddressAndPort.getByName("1"))
                .put("1", InetAddressAndPort.getByName("11"))
                .put("1", InetAddressAndPort.getByName("111"))
                .build();
        Collection<InetAddressAndPort> result = filterBatchlogEndpoints(endpoints);
        // result should be the last two non-local replicas
        // (Collections.shuffle has been replaced with Collections.reverse for testing)
        assertThat(result.size(), is(2));
        assertTrue(result.contains(InetAddressAndPort.getByName("11")));
        assertTrue(result.contains(InetAddressAndPort.getByName("111")));
    }

    @Test
    public void shouldSelectTwoRandomHostsFromSingleRack() throws UnknownHostException
    {
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                .put(LOCAL, InetAddressAndPort.getByName("1"))
                .put(LOCAL, InetAddressAndPort.getByName("11"))
                .put(LOCAL, InetAddressAndPort.getByName("111"))
                .put(LOCAL, InetAddressAndPort.getByName("1111"))
                .build();
        Collection<InetAddressAndPort> result = filterBatchlogEndpoints(endpoints);
        // result should be the last two non-local replicas
        // (Collections.shuffle has been replaced with Collections.reverse for testing)
        assertThat(result.size(), is(2));
        assertTrue(result.contains(InetAddressAndPort.getByName("111")));
        assertTrue(result.contains(InetAddressAndPort.getByName("1111")));
    }

    @Test
    public void shouldSelectOnlyTwoHostsEvenIfLocal() throws UnknownHostException
    {
        Multimap<String, InetAddressAndPort> endpoints = ImmutableMultimap.<String, InetAddressAndPort> builder()
                                                         .put(LOCAL, InetAddressAndPort.getByName("1"))
                                                         .put(LOCAL, InetAddressAndPort.getByName("11"))
                                                         .build();
        Collection<InetAddressAndPort> result = filterBatchlogEndpoints(endpoints);
        assertThat(result.size(), is(2));
        assertTrue(result.contains(InetAddressAndPort.getByName("1")));
        assertTrue(result.contains(InetAddressAndPort.getByName("11")));
    }

    private Collection<InetAddressAndPort> filterBatchlogEndpoints(Multimap<String, InetAddressAndPort> endpoints)
    {
        return ReplicaPlans.filterBatchlogEndpoints(LOCAL, endpoints,
                                                    // Reverse instead of shuffle
                                                    Collections::reverse,
                                                    // Always alive
                                                    (addr) -> true,
                                                    // Always pick the last
                                                    (size) -> size - 1);
    }
}
