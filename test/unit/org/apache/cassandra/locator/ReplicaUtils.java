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

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Assert;

import java.net.UnknownHostException;
import java.util.List;

import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.Replica.transientReplica;

public class ReplicaUtils
{
    public static final Range<Token> FULL_RANGE = new Range<>(Murmur3Partitioner.MINIMUM, Murmur3Partitioner.MINIMUM);
    public static final AbstractBounds<PartitionPosition> FULL_BOUNDS = new Range<>(Murmur3Partitioner.MINIMUM.minKeyBound(), Murmur3Partitioner.MINIMUM.maxKeyBound());

    public static Replica full(InetAddressAndPort endpoint)
    {
        return fullReplica(endpoint, FULL_RANGE);
    }

    public static Replica trans(InetAddressAndPort endpoint)
    {
        return transientReplica(endpoint, FULL_RANGE);
    }

    public static Replica full(InetAddressAndPort endpoint, Token token)
    {
        return fullReplica(endpoint, new Range<>(token, token));
    }

    public static Replica trans(InetAddressAndPort endpoint, Token token)
    {
        return transientReplica(endpoint, new Range<>(token, token));
    }

    static final InetAddressAndPort EP1, EP2, EP3, EP4, EP5, EP6, EP7, EP8, EP9, BROADCAST_EP, NULL_EP;
    static final Range<Token> R1, R2, R3, R4, R5, R6, R7, R8, R9, BROADCAST_RANGE, NULL_RANGE, WRAP_RANGE;
    static final List<InetAddressAndPort> ALL_EP;
    static final List<Range<Token>> ALL_R;

    static
    {
        try
        {
            EP1 = InetAddressAndPort.getByName("127.0.0.1");
            EP2 = InetAddressAndPort.getByName("127.0.0.2");
            EP3 = InetAddressAndPort.getByName("127.0.0.3");
            EP4 = InetAddressAndPort.getByName("127.0.0.4");
            EP5 = InetAddressAndPort.getByName("127.0.0.5");
            EP6 = InetAddressAndPort.getByName("127.0.0.6");
            EP7 = InetAddressAndPort.getByName("127.0.0.7");
            EP8 = InetAddressAndPort.getByName("127.0.0.8");
            EP9 = InetAddressAndPort.getByName("127.0.0.9");
            BROADCAST_EP = FBUtilities.getBroadcastAddressAndPort();
            NULL_EP = InetAddressAndPort.getByName("127.255.255.255");
            R1 = range(0, 1);
            R2 = range(1, 2);
            R3 = range(2, 3);
            R4 = range(3, 4);
            R5 = range(4, 5);
            R6 = range(5, 6);
            R7 = range(6, 7);
            R8 = range(7, 8);
            R9 = range(8, 9);
            BROADCAST_RANGE = range(10, 11);
            NULL_RANGE = range(10000, 10001);
            WRAP_RANGE = range(100000, 0);
            ALL_EP = ImmutableList.of(EP1, EP2, EP3, EP4, EP5, BROADCAST_EP);
            ALL_R = ImmutableList.of(R1, R2, R3, R4, R5, BROADCAST_RANGE, WRAP_RANGE);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    static Token tk(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    static Range<Token> range(long left, long right)
    {
        return new Range<>(tk(left), tk(right));
    }

    static void assertEquals(AbstractReplicaCollection<?> a, AbstractReplicaCollection<?> b)
    {
        Assert.assertEquals(a.list, b.list);
    }

}
