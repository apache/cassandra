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
package org.apache.cassandra.schema;

import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.exceptions.ConfigurationException;
import static java.lang.String.format;

public final class TableParams
{
    public static final TableParams DEFAULT = TableParams.builder().build();

    public enum Option
    {
        BLOOM_FILTER_FP_CHANCE,
        CACHING,
        COMMENT,
        COMPACTION,
        COMPRESSION,
        DCLOCAL_READ_REPAIR_CHANCE,
        DEFAULT_TIME_TO_LIVE,
        EXTENSIONS,
        GC_GRACE_SECONDS,
        MAX_INDEX_INTERVAL,
        MEMTABLE_FLUSH_PERIOD_IN_MS,
        MIN_INDEX_INTERVAL,
        READ_REPAIR_CHANCE,
        SPECULATIVE_RETRY,
        CRC_CHECK_CHANCE;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public static final String DEFAULT_COMMENT = "";
    public static final double DEFAULT_READ_REPAIR_CHANCE = 0.0;
    public static final double DEFAULT_DCLOCAL_READ_REPAIR_CHANCE = 0.1;
    public static final int DEFAULT_GC_GRACE_SECONDS = 864000; // 10 days
    public static final int DEFAULT_DEFAULT_TIME_TO_LIVE = 0;
    public static final int DEFAULT_MEMTABLE_FLUSH_PERIOD_IN_MS = 0;
    public static final int DEFAULT_MIN_INDEX_INTERVAL = 128;
    public static final int DEFAULT_MAX_INDEX_INTERVAL = 2048;
    public static final double DEFAULT_CRC_CHECK_CHANCE = 1.0;

    public final String comment;
    public final double readRepairChance;
    public final double dcLocalReadRepairChance;
    public final double bloomFilterFpChance;
    public final double crcCheckChance;
    public final int gcGraceSeconds;
    public final int defaultTimeToLive;
    public final int memtableFlushPeriodInMs;
    public final int minIndexInterval;
    public final int maxIndexInterval;
    public final SpeculativeRetryParam speculativeRetry;
    public final CachingParams caching;
    public final CompactionParams compaction;
    public final CompressionParams compression;
    public final ImmutableMap<String, ByteBuffer> extensions;

    private TableParams(Builder builder)
    {
        comment = builder.comment;
        readRepairChance = builder.readRepairChance;
        dcLocalReadRepairChance = builder.dcLocalReadRepairChance;
        bloomFilterFpChance = builder.bloomFilterFpChance == null
                            ? builder.compaction.defaultBloomFilterFbChance()
                            : builder.bloomFilterFpChance;
        crcCheckChance = builder.crcCheckChance;
        gcGraceSeconds = builder.gcGraceSeconds;
        defaultTimeToLive = builder.defaultTimeToLive;
        memtableFlushPeriodInMs = builder.memtableFlushPeriodInMs;
        minIndexInterval = builder.minIndexInterval;
        maxIndexInterval = builder.maxIndexInterval;
        speculativeRetry = builder.speculativeRetry;
        caching = builder.caching;
        compaction = builder.compaction;
        compression = builder.compression;
        extensions = builder.extensions;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(TableParams params)
    {
        return new Builder().bloomFilterFpChance(params.bloomFilterFpChance)
                            .caching(params.caching)
                            .comment(params.comment)
                            .compaction(params.compaction)
                            .compression(params.compression)
                            .dcLocalReadRepairChance(params.dcLocalReadRepairChance)
                            .crcCheckChance(params.crcCheckChance)
                            .defaultTimeToLive(params.defaultTimeToLive)
                            .gcGraceSeconds(params.gcGraceSeconds)
                            .maxIndexInterval(params.maxIndexInterval)
                            .memtableFlushPeriodInMs(params.memtableFlushPeriodInMs)
                            .minIndexInterval(params.minIndexInterval)
                            .readRepairChance(params.readRepairChance)
                            .speculativeRetry(params.speculativeRetry)
                            .extensions(params.extensions);
    }

    public void validate()
    {
        compaction.validate();
        compression.validate();

        if (bloomFilterFpChance <= 0 || bloomFilterFpChance > 1)
        {
            fail("%s must be larger than 0.0 and less than or equal to 1.0 (got %s)",
                 Option.BLOOM_FILTER_FP_CHANCE,
                 bloomFilterFpChance);
        }

        if (dcLocalReadRepairChance < 0 || dcLocalReadRepairChance > 1.0)
        {
            fail("%s must be larger than or equal to 0 and smaller than or equal to 1.0 (got %s)",
                 Option.DCLOCAL_READ_REPAIR_CHANCE,
                 dcLocalReadRepairChance);
        }

        if (readRepairChance < 0 || readRepairChance > 1.0)
        {
            fail("%s must be larger than or equal to 0 and smaller than or equal to 1.0 (got %s)",
                 Option.READ_REPAIR_CHANCE,
                 readRepairChance);
        }

        if (crcCheckChance < 0 || crcCheckChance > 1.0)
        {
            fail("%s must be larger than or equal to 0 and smaller than or equal to 1.0 (got %s)",
                 Option.CRC_CHECK_CHANCE,
                 crcCheckChance);
        }

        if (defaultTimeToLive < 0)
            fail("%s must be greater than or equal to 0 (got %s)", Option.DEFAULT_TIME_TO_LIVE, defaultTimeToLive);

        if (gcGraceSeconds < 0)
            fail("%s must be greater than or equal to 0 (got %s)", Option.GC_GRACE_SECONDS, gcGraceSeconds);

        if (minIndexInterval < 1)
            fail("%s must be greater than or equal to 1 (got %s)", Option.MIN_INDEX_INTERVAL, minIndexInterval);

        if (maxIndexInterval < minIndexInterval)
        {
            fail("%s must be greater than or equal to %s (%s) (got %s)",
                 Option.MAX_INDEX_INTERVAL,
                 Option.MIN_INDEX_INTERVAL,
                 minIndexInterval,
                 maxIndexInterval);
        }

        if (memtableFlushPeriodInMs < 0)
            fail("%s must be greater than or equal to 0 (got %s)", Option.MEMTABLE_FLUSH_PERIOD_IN_MS, memtableFlushPeriodInMs);
    }

    private static void fail(String format, Object... args)
    {
        throw new ConfigurationException(format(format, args));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TableParams))
            return false;

        TableParams p = (TableParams) o;

        return comment.equals(p.comment)
            && readRepairChance == p.readRepairChance
            && dcLocalReadRepairChance == p.dcLocalReadRepairChance
            && bloomFilterFpChance == p.bloomFilterFpChance
            && crcCheckChance == p.crcCheckChance
            && gcGraceSeconds == p.gcGraceSeconds
            && defaultTimeToLive == p.defaultTimeToLive
            && memtableFlushPeriodInMs == p.memtableFlushPeriodInMs
            && minIndexInterval == p.minIndexInterval
            && maxIndexInterval == p.maxIndexInterval
            && speculativeRetry.equals(p.speculativeRetry)
            && caching.equals(p.caching)
            && compaction.equals(p.compaction)
            && compression.equals(p.compression)
            && extensions.equals(p.extensions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(comment,
                                readRepairChance,
                                dcLocalReadRepairChance,
                                bloomFilterFpChance,
                                crcCheckChance,
                                gcGraceSeconds,
                                defaultTimeToLive,
                                memtableFlushPeriodInMs,
                                minIndexInterval,
                                maxIndexInterval,
                                speculativeRetry,
                                caching,
                                compaction,
                                compression,
                                extensions);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add(Option.COMMENT.toString(), comment)
                          .add(Option.READ_REPAIR_CHANCE.toString(), readRepairChance)
                          .add(Option.DCLOCAL_READ_REPAIR_CHANCE.toString(), dcLocalReadRepairChance)
                          .add(Option.BLOOM_FILTER_FP_CHANCE.toString(), bloomFilterFpChance)
                          .add(Option.CRC_CHECK_CHANCE.toString(), crcCheckChance)
                          .add(Option.GC_GRACE_SECONDS.toString(), gcGraceSeconds)
                          .add(Option.DEFAULT_TIME_TO_LIVE.toString(), defaultTimeToLive)
                          .add(Option.MEMTABLE_FLUSH_PERIOD_IN_MS.toString(), memtableFlushPeriodInMs)
                          .add(Option.MIN_INDEX_INTERVAL.toString(), minIndexInterval)
                          .add(Option.MAX_INDEX_INTERVAL.toString(), maxIndexInterval)
                          .add(Option.SPECULATIVE_RETRY.toString(), speculativeRetry)
                          .add(Option.CACHING.toString(), caching)
                          .add(Option.COMPACTION.toString(), compaction)
                          .add(Option.COMPRESSION.toString(), compression)
                          .add(Option.EXTENSIONS.toString(), extensions)
                          .toString();
    }

    public static final class Builder
    {
        private String comment = DEFAULT_COMMENT;
        private double readRepairChance = DEFAULT_READ_REPAIR_CHANCE;
        private double dcLocalReadRepairChance = DEFAULT_DCLOCAL_READ_REPAIR_CHANCE;
        private Double bloomFilterFpChance;
        public Double crcCheckChance = DEFAULT_CRC_CHECK_CHANCE;
        private int gcGraceSeconds = DEFAULT_GC_GRACE_SECONDS;
        private int defaultTimeToLive = DEFAULT_DEFAULT_TIME_TO_LIVE;
        private int memtableFlushPeriodInMs = DEFAULT_MEMTABLE_FLUSH_PERIOD_IN_MS;
        private int minIndexInterval = DEFAULT_MIN_INDEX_INTERVAL;
        private int maxIndexInterval = DEFAULT_MAX_INDEX_INTERVAL;
        private SpeculativeRetryParam speculativeRetry = SpeculativeRetryParam.DEFAULT;
        private CachingParams caching = CachingParams.DEFAULT;
        private CompactionParams compaction = CompactionParams.DEFAULT;
        private CompressionParams compression = CompressionParams.DEFAULT;
        private ImmutableMap<String, ByteBuffer> extensions = ImmutableMap.of();

        public Builder()
        {
        }

        public TableParams build()
        {
            return new TableParams(this);
        }

        public Builder comment(String val)
        {
            comment = val;
            return this;
        }

        public Builder readRepairChance(double val)
        {
            readRepairChance = val;
            return this;
        }

        public Builder dcLocalReadRepairChance(double val)
        {
            dcLocalReadRepairChance = val;
            return this;
        }

        public Builder bloomFilterFpChance(double val)
        {
            bloomFilterFpChance = val;
            return this;
        }

        public Builder crcCheckChance(double val)
        {
            crcCheckChance = val;
            return this;
        }

        public Builder gcGraceSeconds(int val)
        {
            gcGraceSeconds = val;
            return this;
        }

        public Builder defaultTimeToLive(int val)
        {
            defaultTimeToLive = val;
            return this;
        }

        public Builder memtableFlushPeriodInMs(int val)
        {
            memtableFlushPeriodInMs = val;
            return this;
        }

        public Builder minIndexInterval(int val)
        {
            minIndexInterval = val;
            return this;
        }

        public Builder maxIndexInterval(int val)
        {
            maxIndexInterval = val;
            return this;
        }

        public Builder speculativeRetry(SpeculativeRetryParam val)
        {
            speculativeRetry = val;
            return this;
        }

        public Builder caching(CachingParams val)
        {
            caching = val;
            return this;
        }

        public Builder compaction(CompactionParams val)
        {
            compaction = val;
            return this;
        }

        public Builder compression(CompressionParams val)
        {
            compression = val;
            return this;
        }

        public Builder extensions(Map<String, ByteBuffer> val)
        {
            extensions = ImmutableMap.copyOf(val);
            return this;
        }
    }
}
