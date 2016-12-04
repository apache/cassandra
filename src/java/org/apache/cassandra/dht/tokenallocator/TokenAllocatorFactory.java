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

package org.apache.cassandra.dht.tokenallocator;

import java.net.InetAddress;
import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

public class TokenAllocatorFactory
{
    private static final Logger logger = LoggerFactory.getLogger(TokenAllocatorFactory.class);
    public static TokenAllocator<InetAddress> createTokenAllocator(NavigableMap<Token, InetAddress> sortedTokens,
                                                     ReplicationStrategy<InetAddress> strategy,
                                                     IPartitioner partitioner)
    {
        if(strategy.replicas() == 1)
        {
            logger.info("Using NoReplicationTokenAllocator.");
            return new NoReplicationTokenAllocator<>(sortedTokens, strategy, partitioner);
        }
        logger.info("Using ReplicationAwareTokenAllocator.");
        return new ReplicationAwareTokenAllocator<>(sortedTokens, strategy, partitioner);
    }
}
