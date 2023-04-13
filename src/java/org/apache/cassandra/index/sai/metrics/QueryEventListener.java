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

/**
 * Listener that gets notified during storage-attached index query execution.
 */
public interface QueryEventListener
{
    interface TrieIndexEventListener
    {
        /**
         * When query potentially matches value range within a segment, and we need to do a traversal.
         */
        void onSegmentHit();

        /**
         * Per-segment trie index traversal time in given units. Recorded when traversal completes.
         */
        void onTraversalComplete(long traversalTotalTime, TimeUnit unit);

        /**
         * Returns events listener for trie postings.
         */
        PostingListEventListener postingListEventListener();
    }

    /**
     * Collector for posting file related metrics.
     */
    interface PostingListEventListener
    {
        /**
         * When an individual posting lists is advanced.
         */
        void onAdvance();

        /**
         * When a posting is successfully read from disk and decoded.
         */
        void postingDecoded(long postingsDecoded);

        PostingListEventListener NO_OP = new PostingListEventListener()
        {
            @Override
            public void onAdvance()
            {

            }

            @Override
            public void postingDecoded(long postingsDecoded)
            {

            }
        };
    }
}
