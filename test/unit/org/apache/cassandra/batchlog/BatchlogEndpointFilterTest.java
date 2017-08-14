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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BatchlogEndpointFilterTest
{
    private static final String LOCAL = "local";

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
        Collection<InetAddress> result = new TestEndpointFilter(LOCAL, endpoints).filter();
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
        Collection<InetAddress> result = new TestEndpointFilter(LOCAL, endpoints).filter();
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
        Collection<InetAddress> result = new TestEndpointFilter(LOCAL, endpoints).filter();
        assertThat(result.size(), is(1));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("0")));
    }

    @Test
    public void shouldSelectTwoRandomHostsFromSingleOtherRack() throws UnknownHostException
    {
        Multimap<String, InetAddress> endpoints = ImmutableMultimap.<String, InetAddress> builder()
                .put(LOCAL, InetAddress.getByName("0"))
                .put(LOCAL, InetAddress.getByName("00"))
                .put("1", InetAddress.getByName("1"))
                .put("1", InetAddress.getByName("11"))
                .put("1", InetAddress.getByName("111"))
                .build();
        Collection<InetAddress> result = new TestEndpointFilter(LOCAL, endpoints).filter();
        // result should be the last two non-local replicas
        // (Collections.shuffle has been replaced with Collections.reverse for testing)
        assertThat(result.size(), is(2));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("11")));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("111")));
    }

    @Test
    public void shouldSelectTwoRandomHostsFromSingleRack() throws UnknownHostException
    {
        Multimap<String, InetAddress> endpoints = ImmutableMultimap.<String, InetAddress> builder()
                .put(LOCAL, InetAddress.getByName("1"))
                .put(LOCAL, InetAddress.getByName("11"))
                .put(LOCAL, InetAddress.getByName("111"))
                .put(LOCAL, InetAddress.getByName("1111"))
                .build();
        Collection<InetAddress> result = new TestEndpointFilter(LOCAL, endpoints).filter();
        // result should be the last two non-local replicas
        // (Collections.shuffle has been replaced with Collections.reverse for testing)
        assertThat(result.size(), is(2));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("111")));
        assertThat(result, JUnitMatchers.hasItem(InetAddress.getByName("1111")));
    }

    private static class TestEndpointFilter extends BatchlogManager.EndpointFilter
    {
        TestEndpointFilter(String localRack, Multimap<String, InetAddress> endpoints)
        {
            super(localRack, endpoints);
        }

        @Override
        protected boolean isValid(InetAddress input)
        {
            // We will use always alive non-localhost endpoints
            return true;
        }

        @Override
        protected int getRandomInt(int bound)
        {
            // We don't need random behavior here
            return bound - 1;
        }

        @Override
        protected void shuffle(List<?> list)
        {
            // We don't need random behavior here
            Collections.reverse(list);
        }
    }
}
