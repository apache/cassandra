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

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaList;
import org.apache.cassandra.service.reads.DigestResolver;

public interface ReadRepair
{
    /**
     * Used by DataResolver to generate corrections as the partition iterator is consumed
     */
    UnfilteredPartitionIterators.MergeListener getMergeListener(Replica[] replicas);

    /**
     * Called when the digests from the initial read don't match. Reads may block on the
     * repair started by this method.
     */
    public void startRepair(DigestResolver digestResolver,
                            ReplicaList allEndpoints,
                            ReplicaList contactedEndpoints,
                            Consumer<PartitionIterator> resultConsumer);

    /**
     * Wait for any operations started by {@link ReadRepair#startRepair} to complete
     */
    public void awaitRepair() throws ReadTimeoutException;

    static ReadRepair create(ReadCommand command, ReplicaList endpoints, long queryStartNanoTime, ConsistencyLevel consistency)
    {
        return new BlockingReadRepair(command, endpoints, queryStartNanoTime, consistency);
    }
}
