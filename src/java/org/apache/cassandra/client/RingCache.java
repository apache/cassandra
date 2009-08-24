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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.IReplicaPlacementStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.CassandraServer;
import org.apache.cassandra.service.StorageService;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import flexjson.JSONTokener;

/**
 *  A class for caching the ring map at the client. For usage example, see
 *  test/unit/org.apache.cassandra.client.TestRingCache.java.
 */
public class RingCache
{
    final private static Logger logger_ = Logger.getLogger(RingCache.class);

    private Set<String> seeds_ = new HashSet<String>();
    final private int port_=DatabaseDescriptor.getThriftPort();
    private volatile IReplicaPlacementStrategy nodePicker_;
    final private static IPartitioner partitioner_ = DatabaseDescriptor.getPartitioner();

    public RingCache()
    {
        seeds_ = DatabaseDescriptor.getSeeds();
        refreshEndPointMap();
    }

    public void refreshEndPointMap()
    {
        for (String seed : seeds_)
        {
            try
            {
                TSocket socket = new TSocket(seed, port_);
                TBinaryProtocol binaryProtocol = new TBinaryProtocol(socket, false, false);
                Cassandra.Client client = new Cassandra.Client(binaryProtocol);
                socket.open();

                Map<String,String> tokenToHostMap = (Map<String,String>) new JSONTokener(client.get_string_property(CassandraServer.TOKEN_MAP)).nextValue();
                
                HashMap<Token, EndPoint> tokenEndpointMap = new HashMap<Token, EndPoint>();
                Map<EndPoint, Token> endpointTokenMap = new HashMap<EndPoint, Token>();
                for (Map.Entry<String,String> entry : tokenToHostMap.entrySet())
                {
                    Token token = StorageService.getPartitioner().getTokenFactory().fromString(entry.getKey());
                    String host = entry.getValue();
                    tokenEndpointMap.put(token, new EndPoint(host, port_));
                    endpointTokenMap.put(new EndPoint(host, port_), token);
                }

                TokenMetadata tokenMetadata = new TokenMetadata(tokenEndpointMap, endpointTokenMap, null);
                Class cls = DatabaseDescriptor.getReplicaPlacementStrategyClass();
                Class [] parameterTypes = new Class[] { TokenMetadata.class, IPartitioner.class, int.class, int.class};
                try
                {
                    nodePicker_ = (IReplicaPlacementStrategy) cls.getConstructor(parameterTypes).newInstance(tokenMetadata, partitioner_, DatabaseDescriptor.getReplicationFactor(), port_);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
                break;
            }
            catch (TException e)
            {
                /* let the Exception go and try another seed. log this though */
                logger_.debug("Error contacting seed " + seed + " " + e.getMessage());
            }
        }
    }

    public EndPoint[] getEndPoint(String key)
    {
        return nodePicker_.getStorageEndPoints(partitioner_.getToken(key));
    }
}
