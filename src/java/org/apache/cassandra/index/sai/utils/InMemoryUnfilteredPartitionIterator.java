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

package org.apache.cassandra.index.sai.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

public class InMemoryUnfilteredPartitionIterator implements UnfilteredPartitionIterator
{
    private final ReadCommand command;
    private final Iterator<Map.Entry<PartitionInfo, TreeSet<Unfiltered>>> partitions;

    public InMemoryUnfilteredPartitionIterator(ReadCommand command, TreeMap<PartitionInfo, TreeSet<Unfiltered>> rowsByPartitions)
    {
        this.command = command;
        this.partitions = rowsByPartitions.entrySet().iterator();
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean hasNext()
    {
        return partitions.hasNext();
    }

    @Override
    public UnfilteredRowIterator next()
    {
        return new InMemoryUnfilteredRowIterator(partitions.next());
    }

    @Override
    public TableMetadata metadata()
    {
        return command.metadata();
    }


    private class InMemoryUnfilteredRowIterator implements UnfilteredRowIterator
    {
        private final PartitionInfo partitionInfo;
        private final Iterator<Unfiltered> unfiltereds;

        public InMemoryUnfilteredRowIterator(Map.Entry<PartitionInfo, TreeSet<Unfiltered>> partition)
        {
            this.partitionInfo = partition.getKey();
            this.unfiltereds = partition.getValue().iterator();
        }

        @Override
        public void close()
        {
        }

        @Override
        public boolean hasNext()
        {
            return unfiltereds.hasNext();
        }

        @Override
        public Unfiltered next()
        {
            return unfiltereds.next();
        }

        @Override
        public TableMetadata metadata()
        {
            return command.metadata();
        }

        @Override
        public boolean isReverseOrder()
        {
            return command.isReversed();
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return command.metadata().regularAndStaticColumns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return partitionInfo.key;
        }

        @Override
        public Row staticRow()
        {
            return partitionInfo.staticRow;
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return partitionInfo.partitionDeletion;
        }

        @Override
        public EncodingStats stats()
        {
            return partitionInfo.encodingStats;
        }
    }
}
