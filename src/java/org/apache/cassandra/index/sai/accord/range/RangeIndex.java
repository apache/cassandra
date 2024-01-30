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

package org.apache.cassandra.index.sai.accord.range;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.segment.IndexSegmentSearcher;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.MemoryIndex;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.schema.IndexMetadata;

public class RangeIndex extends StorageAttachedIndex
{
    public RangeIndex(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
    {
        super(baseCfs, indexMetadata);
    }

    @Override
    protected Strategy createStrategy(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata, IndexTermType indexTermType, IndexIdentifier indexIdentifier)
    {
        return new AbstractStrategy(this)
        {
            @Override
            public MemoryIndex createMemoryIndex()
            {
                return new RangeMemoryIndex(index);
            }

            @Override
            public Flusher flusher()
            {
                return (memtable, indexDescriptor, rowMapping) -> {
                    SegmentMetadata.ComponentMetadataMap metadataMap = memtable.writeDirect(indexDescriptor, indexIdentifier, rowMapping::get);

                    //TODO (now, correctness): size/min/maxRowId are most likely wrong... make sure correct and how to test
                    return new SegmentMetadata(0,
                                               rowMapping.size(),
                                               0,
                                               rowMapping.maxSSTableRowId,
                                               rowMapping.minKey,
                                               rowMapping.maxKey,
                                               memtable.getMinTerm(),
                                               memtable.getMaxTerm(),
                                               metadataMap);
                };
            }

            @Override
            public SegmentBuilder createSegmentBuilder(NamedMemoryLimiter limiter)
            {
                return new RangeSegmentBuilder(index, limiter);
            }

            @Override
            public IndexSegmentSearcher createSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory, PerColumnIndexFiles indexFiles, SegmentMetadata segmentMetadata) throws IOException
            {
                return new RangeIndexSegmentSearcher(primaryKeyMapFactory, indexFiles, segmentMetadata, index);
            }
        };
    }

    //TODO (now): is this still needed as org.apache.cassandra.index.sai.accord.RoutesSearcher bypasses all the filtering?
    @Override
    public void register(IndexRegistry registry)
    {
        registry.registerIndex(this, StorageAttachedIndexGroup.GROUP_KEY, () -> new Group(baseCfs));
    }

    private static final class Group extends StorageAttachedIndexGroup
    {
        protected Group(ColumnFamilyStore baseCfs)
        {
            super(baseCfs);
        }

        @Override
        public StorageAttachedIndexQueryPlan queryPlanFor(RowFilter rowFilter)
        {
            return StorageAttachedIndexQueryPlan.create(baseCfs, queryMetrics, indexes, rowFilter, QueryPlan::new);
        }
    }

    private static final class QueryPlan extends StorageAttachedIndexQueryPlan
    {
        protected QueryPlan(ColumnFamilyStore cfs, TableQueryMetrics queryMetrics, RowFilter postIndexFilter, RowFilter filterOperation, ImmutableSet<Index> indexes)
        {
            super(cfs, queryMetrics, postIndexFilter, filterOperation, indexes);
        }

        @Override
        public Index.Searcher searcherFor(ReadCommand command)
        {
            return new Searcher(cfs,
                                queryMetrics,
                                command,
                                filterOperation,
                                DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
        }
    }

    private static final class Searcher extends StorageAttachedIndexSearcher
    {
        public Searcher(ColumnFamilyStore cfs, TableQueryMetrics tableQueryMetrics, ReadCommand command, RowFilter filterOperation, long executionQuotaMs)
        {
            super(cfs, tableQueryMetrics, command, filterOperation, executionQuotaMs);
        }

        @Override
        public UnfilteredPartitionIterator search(ReadExecutionController executionController) throws RequestTimeoutException
        {
            return new ResultRetriever(queryController, executionController, queryContext, false)
            {
                @Override
                protected boolean allowPostFilter()
                {
                    return false;
                }
            };
        }
    }
}
