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

package org.apache.cassandra.service.reads.repair;

import java.util.function.Consumer;

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.service.reads.DigestResolver;

public class NoopReadRepair implements ReadRepair
{
    public static final NoopReadRepair instance = new NoopReadRepair();

    private NoopReadRepair() {}

    public UnfilteredPartitionIterators.MergeListener getMergeListener(Replica[] replicas)
    {
        return UnfilteredPartitionIterators.MergeListener.NOOP;
    }

    public void startRepair(DigestResolver digestResolver, ReplicaList allReplicas, ReplicaList contactedReplicas, Consumer<PartitionIterator> resultConsumer)
    {
        resultConsumer.accept(digestResolver.getData());
    }

    public void awaitRepair() throws ReadTimeoutException
    {
    }
}
