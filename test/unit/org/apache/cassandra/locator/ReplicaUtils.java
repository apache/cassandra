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

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class ReplicaUtils
{
    private static final Range<Token> FULL_RANGE = new Range<>(Murmur3Partitioner.MINIMUM, Murmur3Partitioner.MINIMUM);

    public static Replica full(InetAddressAndPort endpoint, Range<Token> range)
    {
        return new Replica(endpoint, range, true);
    }

    public static Replica full(InetAddressAndPort endpoint)
    {
        return full(endpoint, FULL_RANGE);
    }

    public static Replica trans(InetAddressAndPort endpoint, Range<Token> range)
    {
        return new Replica(endpoint, range, false);
    }
}
