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

package org.apache.cassandra.service.snapshot;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static java.lang.String.format;

public class SnapshotOptions
{
    public static final String SKIP_FLUSH = "skipFlush";
    public static final String TTL = "ttl";
    public final SnapshotType type;
    public final String tag;
    public final DurationSpec.IntSecondsBound ttl;
    public final Instant creationTime;
    public final boolean skipFlush;
    public final boolean ephemeral;
    public final String[] entities;
    public final RateLimiter rateLimiter;
    public final Predicate<SSTableReader> sstableFilter;
    public final ColumnFamilyStore cfs;

    private SnapshotOptions(Builder builder)
    {
        this.type = builder.type;
        this.tag = builder.tag;
        this.ttl = builder.ttl;
        this.creationTime = builder.creationTime;
        this.skipFlush = builder.skipFlush;
        this.ephemeral = builder.ephemeral;
        this.entities = builder.entities;
        this.rateLimiter = builder.rateLimiter;
        this.sstableFilter = builder.sstableFilter;
        this.cfs = builder.cfs;
    }

    public static Builder systemSnapshot(String tag, SnapshotType type, String... entities)
    {
        return new Builder(tag, type, ssTableReader -> true, entities);
    }

    public static Builder systemSnapshot(String tag, SnapshotType type, Predicate<SSTableReader> sstableFilter, String... entities)
    {
        return new Builder(tag, type, sstableFilter, entities);
    }

    public static SnapshotOptions userSnapshot(String tag, String... entities)
    {
        return userSnapshot(tag, Collections.emptyMap(), entities);
    }

    public static SnapshotOptions userSnapshot(String tag, Map<String, String> options, String... entities)
    {
        Builder builder = new Builder(tag, SnapshotType.USER, ssTableReader -> true, entities).ttl(options.get(TTL));
        if (Boolean.parseBoolean(options.getOrDefault(SKIP_FLUSH, Boolean.FALSE.toString())))
            builder.skipFlush();
        return builder.build();
    }

    public String getSnapshotName(Instant creationTime)
    {
        // Diagnostic snapshots have very specific naming convention hence we are keeping it.
        // Repair snapshots rely on snapshots having name of their repair session ids
        if (type == SnapshotType.USER || type == SnapshotType.DIAGNOSTICS || type == SnapshotType.REPAIR)
            return tag;
        // System snapshots have the creation timestamp on the name
        String snapshotName = format("%d-%s", creationTime.toEpochMilli(), type.label);
        if (StringUtils.isNotBlank(tag))
            snapshotName = snapshotName + '-' + tag;
        return snapshotName;
    }

    public static class Builder
    {
        private final String tag;
        private final String[] entities;
        private DurationSpec.IntSecondsBound ttl;
        private Instant creationTime;
        private boolean skipFlush = false;
        private boolean ephemeral = false;
        private ColumnFamilyStore cfs;
        private final Predicate<SSTableReader> sstableFilter;
        private final SnapshotType type;
        private RateLimiter rateLimiter;

        public Builder(String tag, SnapshotType type, Predicate<SSTableReader> sstableFilter, String... entities)
        {
            this.tag = tag;
            this.type = type;
            this.entities = entities;
            this.sstableFilter = sstableFilter;
        }

        public Builder ttl(String ttl)
        {
            if (ttl != null)
                this.ttl = new DurationSpec.IntSecondsBound(ttl);

            return this;
        }

        public Builder ttl(DurationSpec.IntSecondsBound ttl)
        {
            this.ttl = ttl;
            return this;
        }

        public Builder creationTime(String creationTime)
        {
            if (creationTime != null)
            {
                try
                {
                    return creationTime(Long.parseLong(creationTime));
                }
                catch (Exception ex)
                {
                    throw new RuntimeException("Unable to parse creation time from " + creationTime);
                }
            }

            return this;
        }

        public Builder creationTime(Instant creationTime)
        {
            this.creationTime = creationTime;
            return this;
        }

        public Builder creationTime(long creationTime)
        {
            return creationTime(Instant.ofEpochMilli(creationTime));
        }

        public Builder skipFlush()
        {
            skipFlush = true;
            return this;
        }

        public Builder ephemeral()
        {
            ephemeral = true;
            return this;
        }

        public Builder cfs(ColumnFamilyStore cfs)
        {
            this.cfs = cfs;
            return this;
        }

        public Builder rateLimiter(RateLimiter rateLimiter)
        {
            this.rateLimiter = rateLimiter;
            return this;
        }

        public SnapshotOptions build()
        {
            if (tag == null || tag.isEmpty())
                throw new RuntimeException("You must supply a snapshot name.");

            if (ttl != null)
            {
                int minAllowedTtlSecs = CassandraRelevantProperties.SNAPSHOT_MIN_ALLOWED_TTL_SECONDS.getInt();
                if (ttl.toSeconds() < minAllowedTtlSecs)
                    throw new IllegalArgumentException(format("ttl for snapshot must be at least %d seconds", minAllowedTtlSecs));
            }

            if (ephemeral && ttl != null)
                throw new IllegalStateException(format("can not take ephemeral snapshot (%s) while ttl is specified too", tag));

            if (rateLimiter == null)
                rateLimiter = DatabaseDescriptor.getSnapshotRateLimiter();

            return new SnapshotOptions(this);
        }
    }

    @Override
    public String toString()
    {
        return "CreateSnapshotOptions{" +
               "type=" + type +
               ", tag='" + tag + '\'' +
               ", ttl=" + ttl +
               ", creationTime=" + creationTime +
               ", skipFlush=" + skipFlush +
               ", ephemeral=" + ephemeral +
               ", entities=" + Arrays.toString(entities) +
               '}';
    }
}
