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
package org.apache.cassandra;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.service.StorageService;


/**
 * Convenience object to create updates to a single partition.
 *
 * This is not unlike RowUpdateBuilder except that it allows to create update to multiple rows more easily.
 * It is also aimed at unit tests so favor convenience over efficiency.
 */
public class UpdateBuilder
{
    private final PartitionUpdate update;
    private RowUpdateBuilder currentRow;
    private long timestamp = FBUtilities.timestampMicros();

    private UpdateBuilder(CFMetaData metadata, DecoratedKey partitionKey)
    {
        this.update = new PartitionUpdate(metadata, partitionKey, metadata.partitionColumns(), 4);
    }

    public static UpdateBuilder create(CFMetaData metadata, Object... partitionKey)
    {
        return new UpdateBuilder(metadata, makeKey(metadata, partitionKey));
    }

    public UpdateBuilder withTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
        return this;
    }

    public UpdateBuilder newRow(Object... clustering)
    {
        maybeBuildCurrentRow();
        currentRow = new RowUpdateBuilder(update, timestamp, 0);
        if (clustering.length > 0)
            currentRow.clustering(clustering);
        return this;
    }

    public UpdateBuilder add(String column, Object value)
    {
        assert currentRow != null;
        currentRow.add(column, value);
        return this;
    }

    public PartitionUpdate build()
    {
        maybeBuildCurrentRow();
        return update;
    }

    public IMutation makeMutation()
    {
        Mutation m = new Mutation(build());
        return update.metadata().isCounter()
             ? new CounterMutation(m, ConsistencyLevel.ONE)
             : m;
    }

    public void apply()
    {
        Mutation m = new Mutation(build());
        if (update.metadata().isCounter())
            new CounterMutation(m, ConsistencyLevel.ONE).apply();
        else
            m.apply();
    }

    public void applyUnsafe()
    {
        assert !update.metadata().isCounter() : "Counters have currently no applyUnsafe() option";
        new Mutation(build()).applyUnsafe();
    }

    private void maybeBuildCurrentRow()
    {
        if (currentRow != null)
        {
            currentRow.build();
            currentRow = null;
        }
    }

    private static DecoratedKey makeKey(CFMetaData metadata, Object[] partitionKey)
    {
        if (partitionKey.length == 1 && partitionKey[0] instanceof DecoratedKey)
            return (DecoratedKey)partitionKey[0];

        ByteBuffer key = CFMetaData.serializePartitionKey(metadata.getKeyValidatorAsClusteringComparator().make(partitionKey));
        return metadata.decorateKey(key);
    }
}
