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

package org.apache.cassandra.locator;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;

public class ReplicatedRangeTest
{
    private static Token tk(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    @Test
    public void equalityTest()
    {
        ReplicatedRange rr1Full = new ReplicatedRange(tk(100), tk(200), true);
        ReplicatedRange rr2Full = new ReplicatedRange(tk(200), tk(300), true);

        ReplicatedRange rr1Trans = new ReplicatedRange(tk(100), tk(200), false);
        ReplicatedRange rr2Trans = new ReplicatedRange(tk(200), tk(300), false);

        Assert.assertEquals(rr1Full, rr1Full);
        Assert.assertEquals(rr1Full, rr1Trans);
        Assert.assertNotEquals(rr1Full, rr2Full);
        Assert.assertNotEquals(rr1Full, rr2Trans);
    }
}
