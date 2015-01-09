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
package org.apache.cassandra.db;

import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.service.IReadCommand;

public abstract class AbstractRangeCommand implements IReadCommand
{
    public final String keyspace;
    public final String columnFamily;
    public final long timestamp;

    public final AbstractBounds<RowPosition> keyRange;
    public final IDiskAtomFilter predicate;
    public final List<IndexExpression> rowFilter;

    public AbstractRangeCommand(String keyspace, String columnFamily, long timestamp, AbstractBounds<RowPosition> keyRange, IDiskAtomFilter predicate, List<IndexExpression> rowFilter)
    {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.timestamp = timestamp;
        this.keyRange = keyRange;
        this.predicate = predicate;
        this.rowFilter = rowFilter;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public abstract MessageOut<? extends AbstractRangeCommand> createMessage();
    public abstract AbstractRangeCommand forSubRange(AbstractBounds<RowPosition> range);
    public abstract AbstractRangeCommand withUpdatedLimit(int newLimit);

    public abstract int limit();
    public abstract boolean countCQL3Rows();

    /**
     * Returns true if tombstoned partitions should not be included in results or count towards the limit.
     * See CASSANDRA-8490 for more details on why this is needed (and done this way).
     * */
    public boolean ignoredTombstonedPartitions()
    {
        if (!(predicate instanceof SliceQueryFilter))
            return false;

        return ((SliceQueryFilter) predicate).compositesToGroup == SliceQueryFilter.IGNORE_TOMBSTONED_PARTITIONS;
    }

    public abstract List<Row> executeLocally();

    public long getTimeout()
    {
        return DatabaseDescriptor.getRangeRpcTimeout();
    }
}
