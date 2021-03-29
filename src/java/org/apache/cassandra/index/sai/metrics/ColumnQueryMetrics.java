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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public abstract class ColumnQueryMetrics extends AbstractMetrics
{
    private ColumnQueryMetrics(String indexName, TableMetadata table)
    {
        super(table, indexName, "ColumnQueryMetrics");
    }

    public static class TrieIndexMetrics extends ColumnQueryMetrics implements QueryEventListener.TrieIndexEventListener
    {
        private static final String TRIE_POSTINGS_TYPE = "Postings";

        /**
         * Trie index metrics.
         */
        private final Timer termsTraversalTotalTime;

        private final QueryEventListener.PostingListEventListener postingsListener;

        public TrieIndexMetrics(String indexName, TableMetadata table)
        {
            super(indexName, table);

            termsTraversalTotalTime = Metrics.timer(createMetricName("TermsLookupLatency"));

            Meter postingDecodes = Metrics.meter(createMetricName("PostingDecodes", TRIE_POSTINGS_TYPE));

            postingsListener = new PostingListEventsMetrics(postingDecodes);
        }

        @Override
        public void onSegmentHit() { }

        @Override
        public void onTraversalComplete(long traversalTotalTime, TimeUnit unit)
        {
            termsTraversalTotalTime.update(traversalTotalTime, unit);
        }

        @Override
        public QueryEventListener.PostingListEventListener postingListEventListener()
        {
            return postingsListener;
        }
    }

    public static class BKDIndexMetrics extends ColumnQueryMetrics implements QueryEventListener.BKDIndexEventListener
    {
        private static final String BKD_POSTINGS_TYPE = "KDTreePostings";

        /**
         * BKD index metrics.
         */
        private final Timer intersectionLatency;
        private final Meter postingsNumPostings;
        private final Meter intersectionEarlyExits;

        private final QueryEventListener.PostingListEventListener postingsListener;

        public BKDIndexMetrics(String indexName, TableMetadata table)
        {
            super(indexName, table);

            intersectionLatency = Metrics.timer(createMetricName("KDTreeIntersectionLatency"));
            intersectionEarlyExits = Metrics.meter(createMetricName("KDTreeIntersectionEarlyExits"));

            postingsNumPostings = Metrics.meter(createMetricName("NumPostings", BKD_POSTINGS_TYPE));

            Meter postingDecodes = Metrics.meter(createMetricName("PostingDecodes", BKD_POSTINGS_TYPE));

            postingsListener = new PostingListEventsMetrics(postingDecodes);
        }

        @Override
        public void onIntersectionComplete(long intersectionTotalTime, TimeUnit unit)
        {
            intersectionLatency.update(intersectionTotalTime, unit);
        }

        @Override
        public void onIntersectionEarlyExit()
        {
            intersectionEarlyExits.mark();
        }

        @Override
        public void postingListsHit(int count)
        {
            postingsNumPostings.mark(count);
        }

        @Override
        public void onSegmentHit() { }

        @Override
        public QueryEventListener.PostingListEventListener postingListEventListener()
        {
            return postingsListener;
        }
    }

    private static class PostingListEventsMetrics implements QueryEventListener.PostingListEventListener
    {
        private final Meter postingDecodes;

        private PostingListEventsMetrics(Meter postingDecodes)
        {
            this.postingDecodes = postingDecodes;
        }

        @Override
        public void onAdvance() { }

        @Override
        public void postingDecoded(long postingsDecoded)
        {
            postingDecodes.mark(postingsDecoded);
        }
    }
}
