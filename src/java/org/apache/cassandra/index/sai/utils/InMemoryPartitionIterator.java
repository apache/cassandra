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
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.schema.TableMetadata;

public class InMemoryPartitionIterator implements PartitionIterator
{
    private final ReadCommand command;
    private final Iterator<Map.Entry<PartitionInfo, TreeSet<Unfiltered>>> partitions;

    public InMemoryPartitionIterator(ReadCommand command, TreeMap<PartitionInfo, TreeSet<Unfiltered>> rowsByPartitions)
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
    public RowIterator next()
    {
        return new InMemoryRowIterator(partitions.next());
    }


    private class InMemoryRowIterator implements RowIterator
    {
        private final PartitionInfo partitionInfo;
        private final Iterator<Unfiltered> rows;

        public InMemoryRowIterator(Map.Entry<PartitionInfo, TreeSet<Unfiltered>> rows)
        {
            this.partitionInfo = rows.getKey();
            this.rows = rows.getValue().iterator();
        }

        @Override
        public void close()
        {
        }

        @Override
        public boolean hasNext()
        {
            return rows.hasNext();
        }

        @Override
        public Row next()
        {
            return (Row) rows.next();
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
    }
}
