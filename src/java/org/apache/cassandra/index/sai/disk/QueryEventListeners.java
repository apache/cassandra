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
package org.apache.cassandra.index.sai.disk;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.index.sai.metrics.QueryEventListener;

public class QueryEventListeners
{
    public static final QueryEventListener NO_OP = new BaseQueryEventListener();

    public static final QueryEventListener.BKDIndexEventListener NO_OP_BKD_LISTENER = NO_OP.bkdIndexEventListener();

    public static final QueryEventListener.TrieIndexEventListener NO_OP_TRIE_LISTENER = NO_OP.trieIndexEventListener();

    public static final QueryEventListener.PostingListEventListener NO_OP_POSTINGS_LISTENER = new NoOpPostingListEventListener();

    private static class BaseQueryEventListener implements QueryEventListener
    {
        @Override
        public BKDIndexEventListener bkdIndexEventListener()
        {
            return NoOpBKDIndexEventListener.INSTANCE;
        }

        @Override
        public TrieIndexEventListener trieIndexEventListener()
        {
            return NoOpTrieIndexEventListener.INSTANCE;
        }

        private enum NoOpTrieIndexEventListener implements TrieIndexEventListener
        {
            INSTANCE;

            @Override
            public void onSegmentHit() { }

            @Override
            public void onTraversalComplete(long traversalTotalTime, TimeUnit unit) { }

            @Override
            public PostingListEventListener postingListEventListener()
            {
                return NO_OP_POSTINGS_LISTENER;
            }
        }

        private enum NoOpBKDIndexEventListener implements BKDIndexEventListener
        {
            INSTANCE;

            @Override
            public void onIntersectionComplete(long intersectionTotalTime, TimeUnit unit) { }

            @Override
            public void onIntersectionEarlyExit() { }

            @Override
            public void postingListsHit(int count) { }

            @Override
            public void onSegmentHit() { }

            @Override
            public PostingListEventListener postingListEventListener()
            {
                return NO_OP_POSTINGS_LISTENER;
            }
        }
    }

    public static class NoOpPostingListEventListener implements QueryEventListener.PostingListEventListener
    {
        @Override
        public void onAdvance() { }

        @Override
        public void onPostingDecoded() { }
    }
}
