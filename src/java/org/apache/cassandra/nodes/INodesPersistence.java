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

package org.apache.cassandra.nodes;

import java.util.stream.Stream;

import org.apache.cassandra.locator.InetAddressAndPort;

public interface INodesPersistence
{
    public static final INodesPersistence NO_NODES_PERSISTENCE = new INodesPersistence()
    {
        @Override
        public LocalInfo loadLocal()
        {
            return null;
        }

        @Override
        public void saveLocal(LocalInfo info)
        {
            // no-op
        }

        @Override
        public void syncLocal()
        {
            // no-op
        }

        @Override
        public Stream<PeerInfo> loadPeers()
        {
            return Stream.empty();
        }

        @Override
        public void savePeer(PeerInfo info)
        {
            // no-op
        }

        @Override
        public void deletePeer(InetAddressAndPort endpoint)
        {
            // no-op
        }

        @Override
        public void syncPeers()
        {
            // no-op
        }
    };

    LocalInfo loadLocal();

    void saveLocal(LocalInfo info);

    void syncLocal();

    Stream<PeerInfo> loadPeers();

    void savePeer(PeerInfo info);

    void deletePeer(InetAddressAndPort endpoint);

    void syncPeers();
}
