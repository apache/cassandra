/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.CleanupHelper;
import static org.apache.cassandra.Util.range;
import static org.apache.cassandra.Util.bounds;
import static org.apache.cassandra.Util.token;

import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.locator.TokenMetadata;

public class StorageProxyTest extends CleanupHelper
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token("1"), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(token("6"), InetAddress.getByName("127.0.0.6"));
    }

    private void testGRR(AbstractBounds queryRange, AbstractBounds... expected)
    {
        List<AbstractBounds> restricted = StorageProxy.getRestrictedRanges(queryRange);
        assertEquals(restricted.toString(), expected.length, restricted.size());
        for (int i = 0; i < expected.length; i++)
            assertEquals("Mismatch for index " + i + ": " + restricted, expected[i], restricted.get(i));
    }

    @Test
    public void testGRR() throws Throwable
    {
        // no splits
        testGRR(range("2", "5"), range("2", "5"));
        testGRR(bounds("2", "5"), bounds("2", "5"));
        // single split
        testGRR(range("2", "7"), range("2", "6"), range("6", "7"));
        testGRR(bounds("2", "7"), bounds("2", "6"), range("6", "7"));
        // single split starting from min
        testGRR(range("", "2"), range("", "1"), range("1", "2"));
        testGRR(bounds("", "2"), bounds("", "1"), range("1", "2"));
        // single split ending with max
        testGRR(range("5", ""), range("5", "6"), range("6", ""));
        testGRR(bounds("5", ""), bounds("5", "6"), range("6", ""));
        // two splits
        testGRR(range("0", "7"), range("0", "1"), range("1", "6"), range("6", "7"));
        testGRR(bounds("0", "7"), bounds("0", "1"), range("1", "6"), range("6", "7"));
    }

    @Test
    public void testGRRExact() throws Throwable
    {
        // min
        testGRR(range("1", "5"), range("1", "5"));
        testGRR(bounds("1", "5"), bounds("1", "1"), range("1", "5"));
        // max
        testGRR(range("2", "6"), range("2", "6"));
        testGRR(bounds("2", "6"), bounds("2", "6"));
        // both
        testGRR(range("1", "6"), range("1", "6"));
        testGRR(bounds("1", "6"), bounds("1", "1"), range("1", "6"));
    }

    @Test
    public void testGRRWrapped() throws Throwable
    {
        // one token in wrapped range
        testGRR(range("7", "0"), range("7", ""), range("", "0"));
        // two tokens in wrapped range
        testGRR(range("5", "0"), range("5", "6"), range("6", ""), range("", "0"));
        testGRR(range("7", "2"), range("7", ""), range("", "1"), range("1", "2"));
        // full wraps
        testGRR(range("0", "0"), range("0", "1"), range("1", "6"), range("6", ""), range("", "0"));
        testGRR(range("", ""), range("", "1"), range("1", "6"), range("6", ""));
        // wrap on member tokens
        testGRR(range("6", "6"), range("6", ""), range("", "1"), range("1", "6"));
        testGRR(range("6", "1"), range("6", ""), range("", "1"));
        // end wrapped
        testGRR(range("5", ""), range("5", "6"), range("6", ""));
    }

    @Test
    public void testGRRExactBounds() throws Throwable
    {
        // equal tokens are special cased as non-wrapping for bounds
        testGRR(bounds("0", "0"), bounds("0", "0"));
        // completely empty bounds match everything
        testGRR(bounds("", ""), bounds("", "1"), range("1", "6"), range("6", ""));
    }
}
