/**
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
package org.apache.cassandra.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * A class for caching the ring map at the client. For usage example, see
 * test/unit/org.apache.cassandra.client.TestRingCache.java.
 * TODO: doing a naive linear search of the token map
 */
public class RingCache
{
    final private static Logger logger_ = LoggerFactory.getLogger(RingCache.class);

    private final IPartitioner<?> partitioner;
    private final Configuration conf;

    private Multimap<Range, InetAddress> rangeMap;

    public RingCache(Configuration conf) throws IOException
    {
        this.conf = conf;
        this.partitioner = ConfigHelper.getPartitioner(conf);
        refreshEndpointMap();
    }

    public void refreshEndpointMap()
    {
            try {
                
                Cassandra.Client client = ConfigHelper.getClientFromAddressList(conf);

                List<TokenRange> ring = client.describe_ring(ConfigHelper.getOutputKeyspace(conf));
                rangeMap = ArrayListMultimap.create();
                
                for (TokenRange range : ring)
                {
                    Token<?> left = partitioner.getTokenFactory().fromString(range.start_token);
                    Token<?> right = partitioner.getTokenFactory().fromString(range.end_token);
                    Range r = new Range(left, right, partitioner);
                    for (String host : range.endpoints)
                    {
                        try
                        {
                            rangeMap.put(r, InetAddress.getByName(host));
                        }
                        catch (UnknownHostException e)
                        {
                            throw new AssertionError(e); // host strings are IPs
                        }
                    }
                }
            }
            catch (InvalidRequestException e)
            {
                throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            catch (TException e)
            {
                logger_.debug("Error contacting seed list" + ConfigHelper.getInitialAddress(conf) + " " + e.getMessage());
            }
        }

    /** ListMultimap promises to return a List for get(K) */
    public List<InetAddress> getEndpoint(Range range)
    {
        return (List<InetAddress>) rangeMap.get(range);
    }

    public List<InetAddress> getEndpoint(ByteBuffer key)
    {
        return getEndpoint(getRange(key));
    }

    public Range getRange(ByteBuffer key)
    {
        // TODO: naive linear search of the token map
        Token<?> t = partitioner.getToken(key);
        for (Range range : rangeMap.keySet())
            if (range.contains(t))
                return range;

        throw new RuntimeException("Invalid token information returned by describe_ring: " + rangeMap);
    }
}
