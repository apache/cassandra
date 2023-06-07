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

package org.apache.cassandra.service.accord;

import accord.messages.Request;
import accord.primitives.Txn;
import accord.topology.TopologyManager;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface IAccordService
{
    IVerbHandler<? extends Request> verbHandler();

    TxnData coordinate(Txn txn, ConsistencyLevel consistencyLevel);

    long currentEpoch();

    void setCacheSize(long kb);

    TopologyManager topology();

    void startup();

    void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;

    /**
     * Return a future that will complete once the accord has completed it's local bootstrap process
     * for any ranges gained in the given epoch
     */
    Future<Void> epochReady(Epoch epoch);

    void remoteSyncComplete(Message<AccordLocalSyncNotifier.Notification> message);

    /**
     * Temporary method to avoid double-streaming keyspaces
     * @param keyspace
     * @return
     */
    boolean isAccordManagedKeyspace(String keyspace);
}
