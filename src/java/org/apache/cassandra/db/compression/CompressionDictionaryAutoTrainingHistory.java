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

package org.apache.cassandra.db.compression;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Node-local record of the auto-training adoption decisions this node has made, exposed by the
 * {@code system_views.compression_dictionary_auto_training} virtual table.
 * <p>
 * Auto-training evaluates a freshly trained candidate dictionary against the table's current one and adopts the
 * candidate only if it improves the compression ratio by at least the configured threshold. That decision is
 * otherwise only visible in the log, which makes it awkward to answer "has this table ever produced a better
 * dictionary, and by how much". Each decision is kept here instead.
 * <p>
 * The history is held in memory only: it is bounded, and a restart starts afresh. Only the node that ran the
 * training records anything, so a cluster-wide picture means querying every node.
 */
public class CompressionDictionaryAutoTrainingHistory
{
    public static final int DEFAULT_MAX_ENTRIES = 1000;

    public static final CompressionDictionaryAutoTrainingHistory instance =
        new CompressionDictionaryAutoTrainingHistory(DEFAULT_MAX_ENTRIES);

    private final int maxEntries;
    // newest first, so the virtual table and the eviction of the oldest entry both read naturally
    private final Deque<Entry> entries = new ArrayDeque<>();

    @VisibleForTesting
    public CompressionDictionaryAutoTrainingHistory(int maxEntries)
    {
        this.maxEntries = maxEntries;
    }

    /**
     * Records one adoption decision.
     *
     * @param keyspaceName   keyspace the candidate was trained for
     * @param tableName      table the candidate was trained for
     * @param kind           kind of the dictionary that was trained
     * @param baselineRatio  compressed/uncompressed ratio the current dictionary achieved on the sample
     * @param candidateRatio compressed/uncompressed ratio the candidate achieved on the same sample
     * @param improvement    relative improvement of the candidate over the baseline
     * @param threshold      improvement the candidate had to reach to be adopted
     * @param promoted       whether the candidate became the table's dictionary
     */
    public void record(String keyspaceName,
                       String tableName,
                       CompressionDictionary.Kind kind,
                       double baselineRatio,
                       double candidateRatio,
                       double improvement,
                       double threshold,
                       boolean promoted)
    {
        record(FBUtilities.now().toEpochMilli(), keyspaceName, tableName, kind,
               baselineRatio, candidateRatio, improvement, threshold, promoted);
    }

    /**
     * Records a decision at an explicit time. A table trains one candidate at a time and training is far slower
     * than the millisecond resolution of the timestamp, so in production two decisions for one table cannot share
     * a timestamp; tests that need several entries have to space them out themselves.
     */
    @VisibleForTesting
    public void record(long timestampMillis,
                       String keyspaceName,
                       String tableName,
                       CompressionDictionary.Kind kind,
                       double baselineRatio,
                       double candidateRatio,
                       double improvement,
                       double threshold,
                       boolean promoted)
    {
        Entry entry = new Entry(timestampMillis,
                                FBUtilities.getBroadcastAddressAndPort(),
                                keyspaceName,
                                tableName,
                                kind,
                                baselineRatio,
                                candidateRatio,
                                improvement,
                                threshold,
                                promoted);

        synchronized (entries)
        {
            if (entries.size() == maxEntries)
                entries.removeLast();

            entries.addFirst(entry);
        }
    }

    /**
     * @return the recorded decisions, newest first
     */
    public List<Entry> entries()
    {
        synchronized (entries)
        {
            return new ArrayList<>(entries);
        }
    }

    @VisibleForTesting
    public void clear()
    {
        synchronized (entries)
        {
            entries.clear();
        }
    }

    public static class Entry
    {
        public final long timestampMillis;
        public final InetAddressAndPort node;
        public final String keyspaceName;
        public final String tableName;
        public final CompressionDictionary.Kind kind;
        public final double baselineRatio;
        public final double candidateRatio;
        public final double improvement;
        public final double threshold;
        public final boolean promoted;

        Entry(long timestampMillis,
              InetAddressAndPort node,
              String keyspaceName,
              String tableName,
              CompressionDictionary.Kind kind,
              double baselineRatio,
              double candidateRatio,
              double improvement,
              double threshold,
              boolean promoted)
        {
            this.timestampMillis = timestampMillis;
            this.node = node;
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.kind = kind;
            this.baselineRatio = baselineRatio;
            this.candidateRatio = candidateRatio;
            this.improvement = improvement;
            this.threshold = threshold;
            this.promoted = promoted;
        }
    }
}
