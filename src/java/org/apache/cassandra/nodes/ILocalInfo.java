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

import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.CassandraVersion;

public interface ILocalInfo extends INodeInfo<LocalInfo>
{
    InetAddressAndPort getBroadcastAddressAndPort();

    SystemKeyspace.BootstrapState getBootstrapState();

    String getClusterName();

    CassandraVersion getCqlVersion();

    InetAddressAndPort getListenAddressAndPort();

    ProtocolVersion getNativeProtocolVersion();

    Class<? extends IPartitioner> getPartitionerClass();

    ImmutableMap<UUID, TruncationRecord> getTruncationRecords();

    @Override
    LocalInfo duplicate();
}
