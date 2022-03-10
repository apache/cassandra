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
package org.apache.cassandra.index.sai.metrics;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.TableMetadata;

public class QueryEventListeners
{
    public static final ColumnQueryMetrics NO_OP_BKD_LISTENER = new NoOpBkdIndexEventListener();

    public static final ColumnQueryMetrics NO_OP_TRIE_LISTENER = new NoOpTrieIndexEventListener();

    public static final QueryEventListener.PostingListEventListener NO_OP_POSTINGS_LISTENER = new NoOpPostingListEventListener();

    private static class NoOpTrieIndexEventListener extends ColumnQueryMetrics implements QueryEventListener.TrieIndexEventListener
    {
        NoOpTrieIndexEventListener()
        {
            super(null, TableMetadata.builder("ks", "tb").addPartitionKeyColumn("pk", UTF8Type.instance).build());
        }

        @Override
        public void onSegmentHit()
        {}

        @Override
        public void onTraversalComplete(long traversalTotalTime, TimeUnit unit)
        {}

        @Override
        public QueryEventListener.PostingListEventListener postingListEventListener()
        {
            return NO_OP_POSTINGS_LISTENER;
        }
    }

    private static class NoOpBkdIndexEventListener extends ColumnQueryMetrics implements QueryEventListener.BKDIndexEventListener
    {
        NoOpBkdIndexEventListener()
        {
            super(null, TableMetadata.builder("ks", "tb").addPartitionKeyColumn("pk", UTF8Type.instance).build());
        }

        @Override
        public void onIntersectionComplete(long intersectionTotalTime, TimeUnit unit)
        {}

        @Override
        public void onIntersectionEarlyExit()
        {}

        @Override
        public void postingListsHit(int count)
        {}

        @Override
        public void onSegmentHit()
        {}

        @Override
        public QueryEventListener.PostingListEventListener postingListEventListener()
        {
            return NO_OP_POSTINGS_LISTENER;
        }
    }

    public static class NoOpPostingListEventListener implements QueryEventListener.PostingListEventListener
    {
        @Override
        public void onAdvance() { }

        @Override
        public void postingDecoded(long postingsDecoded) { }
    }
}
