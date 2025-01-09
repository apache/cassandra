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

package org.apache.cassandra.replication;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.schema.TableId;

/**
 * Used to ensure that the data and mutation summaries returned by a logged read command accurately describe
 * each other. That is, we're not returning data that contains mutations not reflected in the summary, and we're
 * not including mutations in the summary that aren't reflected in the data.
 */
public interface MutationSummarizer extends AutoCloseable
{
    static MutationSummarizer NOOP = new MutationSummarizer()
    {
        @Override
        public void addForKey(TableId table, DecoratedKey key) {}

        @Override
        public void addForRange(TableId table, AbstractBounds<PartitionPosition> range) {}

        @Override
        public MutationSummary summary() { return null; }

        @Override
        public void close() {}
    };

    // TODO: accept sstable/memtable id data to accumulate
    void addForKey(TableId table, DecoratedKey key);

    void addForRange(TableId table, AbstractBounds<PartitionPosition> range);

    MutationSummary summary();

    @Override
    void close();
}
