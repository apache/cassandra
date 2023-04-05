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

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;


/**
 * Convenience object to create updates to a single partition.
 *
 * This is not unlike RowUpdateBuilder except that it allows to create update to multiple rows more easily.
 * It is also aimed at unit tests so favor convenience over efficiency.
 */
public class UpdateBuilder
{
    private final PartitionUpdate.SimpleBuilder updateBuilder;
    private Row.SimpleBuilder currentRow;

    private UpdateBuilder(PartitionUpdate.SimpleBuilder updateBuilder)
    {
        this.updateBuilder = updateBuilder;
    }

    public static UpdateBuilder create(TableMetadata metadata, Object... partitionKey)
    {
        return new UpdateBuilder(PartitionUpdate.simpleBuilder(metadata, partitionKey));
    }

    public UpdateBuilder withTimestamp(long timestamp)
    {
        updateBuilder.timestamp(timestamp);
        if (currentRow != null)
            currentRow.timestamp(timestamp);
        return this;
    }

    public UpdateBuilder newRow(Object... clustering)
    {
        currentRow = updateBuilder.row(clustering);
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
        return updateBuilder.build();
    }

    public IMutation makeMutation()
    {
        Mutation m = updateBuilder.buildAsMutation();
        return updateBuilder.metadata().isCounter()
             ? new CounterMutation(m, ConsistencyLevel.ONE)
             : m;
    }

    public void apply()
    {
        makeMutation().apply();
    }

    public void applyUnsafe()
    {
        assert !updateBuilder.metadata().isCounter() : "Counters have currently no applyUnsafe() option";
        updateBuilder.buildAsMutation().applyUnsafe();
    }
}
