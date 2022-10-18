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

import org.apache.cassandra.index.sai.QueryContext;

public final class MulticastQueryEventListeners
{
    public static QueryEventListener.TrieIndexEventListener of(QueryContext ctx, QueryEventListener.TrieIndexEventListener listener)
    {
        return new Multicast2TrieIndexEventListener(ctx, listener);
    }

    public static QueryEventListener.BKDIndexEventListener of(QueryContext ctx, QueryEventListener.BKDIndexEventListener listener)
    {
        return new Multicast2BKDIndexEventListener(ctx, listener);
    }

    public static class Multicast2TrieIndexEventListener implements QueryEventListener.TrieIndexEventListener
    {
        private final QueryContext ctx;
        private final QueryEventListener.TrieIndexEventListener listener;
        private final Multicast2TriePostingListEventListener postingListEventListener;

        private Multicast2TrieIndexEventListener(QueryContext ctx, QueryEventListener.TrieIndexEventListener listener)
        {
            this.ctx = ctx;
            this.listener = listener;
            this.postingListEventListener = new Multicast2TriePostingListEventListener(ctx, listener.postingListEventListener());
        }

        @Override
        public void onSegmentHit()
        {
            ctx.segmentsHit++;
            ctx.trieSegmentsHit++;
            listener.onSegmentHit();
        }

        @Override
        public void onTraversalComplete(long traversalTotalTime, TimeUnit unit)
        {
            listener.onTraversalComplete(traversalTotalTime, unit);
        }

        @Override
        public QueryEventListener.PostingListEventListener postingListEventListener()
        {
            return postingListEventListener;
        }
    }

    public static class Multicast2BKDIndexEventListener implements QueryEventListener.BKDIndexEventListener
    {
        private final QueryContext ctx;
        private final QueryEventListener.BKDIndexEventListener listener;
        private final Multicast2BKDPostingListEventListener postingListEventListener;

        private Multicast2BKDIndexEventListener(QueryContext ctx, QueryEventListener.BKDIndexEventListener listener)
        {
            this.ctx = ctx;
            this.listener = listener;
            this.postingListEventListener = new Multicast2BKDPostingListEventListener(ctx, listener.postingListEventListener());
        }

        @Override
        public void onIntersectionComplete(long intersectionTotalTime, TimeUnit unit)
        {
            listener.onIntersectionComplete(intersectionTotalTime, unit);
        }

        @Override
        public void onIntersectionEarlyExit()
        {
            listener.onIntersectionEarlyExit();
        }

        @Override
        public void postingListsHit(int count)
        {
            ctx.bkdPostingListsHit++;
            listener.postingListsHit(count);
        }

        @Override
        public void onSegmentHit()
        {
            ctx.segmentsHit++;
            ctx.bkdSegmentsHit++;
            listener.onSegmentHit();
        }

        @Override
        public QueryEventListener.PostingListEventListener postingListEventListener()
        {
            return postingListEventListener;
        }
    }

    public static class Multicast2BKDPostingListEventListener implements QueryEventListener.PostingListEventListener
    {
        private final QueryContext ctx;
        private final QueryEventListener.PostingListEventListener listener;

        Multicast2BKDPostingListEventListener(QueryContext ctx, QueryEventListener.PostingListEventListener listener)
        {
            this.ctx = ctx;
            this.listener = listener;
        }

        @Override
        public void onAdvance()
        {
            ctx.bkdPostingsSkips++;
            listener.onAdvance();
        }

        @Override
        public void onPostingDecoded()
        {
            ctx.bkdPostingsDecodes++;
            listener.onPostingDecoded();
        }
    }

    public static class Multicast2TriePostingListEventListener implements QueryEventListener.PostingListEventListener
    {
        private final QueryContext ctx;
        private final QueryEventListener.PostingListEventListener listener;

        Multicast2TriePostingListEventListener(QueryContext ctx, QueryEventListener.PostingListEventListener listener)
        {
            this.ctx = ctx;
            this.listener = listener;
        }

        @Override
        public void onAdvance()
        {
            ctx.triePostingsSkips++;
            listener.onAdvance();
        }

        @Override
        public void onPostingDecoded()
        {
            ctx.triePostingsDecodes++;
            listener.onPostingDecoded();
        }
    }
}
