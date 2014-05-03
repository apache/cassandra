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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.junit.Test;
import org.junit.matchers.JUnitMatchers;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

public class BatchlogEndpointSelectorTest
{
    private final BatchlogEndpointSelector target;
    private static final String LOCAL = "local";
    

    public BatchlogEndpointSelectorTest() throws UnknownHostException
    {
        target = new BatchlogEndpointSelector(LOCAL)
        {
            @Override
            protected boolean isValid(InetAddress input)
            {   
                //we will use always alive non-localhost endpoints
                return true;
            }

            @Override
            protected int getRandomInt(int bound)
            {
                //we don't need a random behavior here
                return bound - 1;
            }
        };
    }
    
    @Test
    public void shouldSelect2hostsFromNonLocalRacks() throws UnknownHostException
    {
        Multimap<String, InetAddress> endpoints = ImmutableMultimap.<String, InetAddress> builder()
                .put(LOCAL, InetAddress.getByName("0"))
                .put(LOCAL, InetAddress.getByName("00"))
                .put("1", InetAddress.getByName("1"))
                .put("1", InetAddress.getByName("11"))
                .put("2", InetAddress.getByName("2"))
                .put("2", InetAddress.getByName("22"))
                .build();
        Collection<InetAddress> result = target.chooseEndpoints(endpoints);
        assertThat(result.size(), is(2));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("11")));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("22")));
    }
    
    @Test
    public void shouldSelectHostFromLocal() throws UnknownHostException
    {
        Multimap<String, InetAddress> endpoints = ImmutableMultimap.<String, InetAddress> builder()
                .put(LOCAL, InetAddress.getByName("0"))
                .put(LOCAL, InetAddress.getByName("00"))
                .put("1", InetAddress.getByName("1"))
                .build();
        Collection<InetAddress> result = target.chooseEndpoints(endpoints);
        assertThat(result.size(), is(2));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("1")));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("0")));
    }
    
    @Test
    public void shouldReturnAsIsIfNoEnoughEndpoints() throws UnknownHostException
    {
        Multimap<String, InetAddress> endpoints = ImmutableMultimap.<String, InetAddress> builder()
                .put(LOCAL, InetAddress.getByName("0"))
                .build();
        Collection<InetAddress> result = target.chooseEndpoints(endpoints);
        assertThat(result.size(), is(1));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("0")));
    }
    
    @Test
    public void shouldSelectTwoFirstHostsFromSingleOtherRack() throws UnknownHostException
    {
        Multimap<String, InetAddress> endpoints = ImmutableMultimap.<String, InetAddress> builder()
                .put(LOCAL, InetAddress.getByName("0"))
                .put(LOCAL, InetAddress.getByName("00"))
                .put("1", InetAddress.getByName("1"))
                .put("1", InetAddress.getByName("11"))
                .put("1", InetAddress.getByName("111"))
                .build();
        Collection<InetAddress> result = target.chooseEndpoints(endpoints);
        assertThat(result.size(), is(2));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("1")));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("11")));
    }
}
