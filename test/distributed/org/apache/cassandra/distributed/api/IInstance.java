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

package org.apache.cassandra.distributed.api;

import org.apache.cassandra.locator.InetAddressAndPort;

import java.util.UUID;
import java.util.concurrent.Future;

// The cross-version API requires that an Instance has a constructor signature of (IInstanceConfig, ClassLoader)
public interface IInstance extends IIsolatedExecutor
{
    ICoordinator coordinator();
    IListen listen();

    void schemaChangeInternal(String query);
    public Object[][] executeInternal(String query, Object... args);

    IInstanceConfig config();
    public InetAddressAndPort broadcastAddressAndPort();
    UUID schemaVersion();

    void startup();
    boolean isShutdown();
    Future<Void> shutdown();
    Future<Void> shutdown(boolean graceful);

    int liveMemberCount();

    int nodetool(String... commandAndArgs);

    // these methods are not for external use, but for simplicity we leave them public and on the normal IInstance interface
    void startup(ICluster cluster);
    void receiveMessage(IMessage message);

    int getMessagingVersion();
    void setMessagingVersion(InetAddressAndPort endpoint, int version);

    void flush(String keyspace);
    void forceCompact(String keyspace, String table);
}
