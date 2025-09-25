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

package org.apache.cassandra.distributed.test.tracking;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.journal.DeserializedRecordConsumer;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.ShortMutationId;
import org.junit.Assert;

// Separate class for MutationTrackingBounceTest, since without it we were getting non-serializable class exceptions, likely due to static fields
public class MutationTrackingBounce_ValidateRunnable implements IIsolatedExecutor.SerializableRunnable
{
    private final int count;

    public MutationTrackingBounce_ValidateRunnable(int expectedMutations)
    {
        this.count = expectedMutations;
    }

    @Override
    public void run()
    {
        AtomicInteger counter = new AtomicInteger();
        MutationJournal.instance().replay(new DeserializedRecordConsumer<>(MutationJournal.MutationSerializer.INSTANCE)
        {
            Set<ShortMutationId> seen = new HashSet<>();
            @Override
            protected void accept(long segment, int position, ShortMutationId key, Mutation mutation)
            {
                if (!seen.add(key))
                    throw new AssertionError(String.format("Should have witnessed each key just once, but seen %s already", key));

                for (PartitionUpdate partitionUpdate : mutation.getPartitionUpdates())
                {
                    if (!MutationTrackingService.instance().createSummaryForKey(partitionUpdate.partitionKey(), partitionUpdate.metadata().id, false)
                                                         .contains(key))
                    {
                        throw new AssertionError(String.format("Mutation %s should have been witnessed (%s)", mutation, key));
                    }
                }
                counter.incrementAndGet();
            }
        }, 1);
        Assert.assertEquals(count, counter.get());
    }
}
