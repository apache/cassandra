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

package org.apache.cassandra.tcm;

import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.ExtensionValue;

public class MetadataKeys
{
    public static final String CORE_NS = MetadataKeys.class.getPackage().getName().toLowerCase(Locale.ROOT);

    public static final MetadataKey SCHEMA                  = make(CORE_NS, "schema", "dist_schema");
    public static final MetadataKey NODE_DIRECTORY          = make(CORE_NS, "membership", "node_directory");
    public static final MetadataKey TOKEN_MAP               = make(CORE_NS, "ownership", "token_map");
    public static final MetadataKey DATA_PLACEMENTS         = make(CORE_NS, "ownership", "data_placements");
    public static final MetadataKey ACCORD_FAST_PATH        = make(CORE_NS, "ownership", "accord_fast_path");
    public static final MetadataKey ACCORD_STALE_REPLICAS   = make(CORE_NS, "ownership", "accord_stale_replicas");
    public static final MetadataKey LOCKED_RANGES           = make(CORE_NS, "sequences", "locked_ranges");
    public static final MetadataKey IN_PROGRESS_SEQUENCES   = make(CORE_NS, "sequences", "in_progress");
    public static final MetadataKey CONSENSUS_MIGRATION_STATE = make(CORE_NS, "consensus", "migration_state");

    public static final ImmutableMap<MetadataKey, Function<ClusterMetadata, MetadataValue<?>>> CORE_METADATA
    = ImmutableMap.<MetadataKey, Function<ClusterMetadata, MetadataValue<?>>>builder()
                  .put(SCHEMA, cm -> cm.schema)
                  .put(NODE_DIRECTORY, cm -> cm.directory)
                  .put(TOKEN_MAP, cm -> cm.tokenMap)
                  .put(DATA_PLACEMENTS, cm -> cm.placements)
                  .put(LOCKED_RANGES, cm -> cm.lockedRanges)
                  .put(IN_PROGRESS_SEQUENCES, cm -> cm.inProgressSequences)
                  .put(ACCORD_FAST_PATH, cm -> cm.accordFastPath)
                  .put(ACCORD_STALE_REPLICAS, cm -> cm.accordStaleReplicas)
                  .put(CONSENSUS_MIGRATION_STATE, cm -> cm.consensusMigrationState)
                  .build();

    public static MetadataKey make(String...parts)
    {
        assert parts != null && parts.length >= 1;
        StringBuilder b = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++)
        {
            b.append('.');
            b.append(parts[i]);
        }
        return new MetadataKey(b.toString());
    }

    public static MetadataValue<?> extract(ClusterMetadata cm, MetadataKey key)
    {
        if (CORE_METADATA.containsKey(key))
            return CORE_METADATA.get(key).apply(cm);
        if (!(key instanceof ExtensionKey<?, ?>))
            throw new IllegalArgumentException("Unknown key: " + key);
        return cm.extensions.get(key);
    }

    public static ImmutableSet<MetadataKey> diffKeys(ClusterMetadata before, ClusterMetadata after)
    {
        ImmutableSet.Builder<MetadataKey> builder = new ImmutableSet.Builder<>();
        diffKeys(before, after, builder);
        return builder.build();
    }

    private static void diffKeys(ClusterMetadata before, ClusterMetadata after, ImmutableSet.Builder<MetadataKey> builder)
    {
        for (Map.Entry<MetadataKey, Function<ClusterMetadata, MetadataValue<?>>> e : CORE_METADATA.entrySet())
            checkKey(before, after, builder, e.getValue(), e.getKey());

        Set<ExtensionKey<?,?>> added = new HashSet<>(after.extensions.keySet());
        for (Map.Entry<ExtensionKey<?, ?>, ExtensionValue<?>> entry : before.extensions.entrySet())
        {
            ExtensionKey<?, ?> key = entry.getKey();
            added.remove(key);

            if (after.extensions.containsKey(key))
                checkKey(before, after, builder, cm -> cm.extensions.get(key), key);
            else
                builder.add(key);
        }

        for (ExtensionKey<?, ?> key : added)
            builder.add(key);
    }

    private static void checkKey(ClusterMetadata before, ClusterMetadata after, ImmutableSet.Builder<MetadataKey> builder, Function<ClusterMetadata, MetadataValue<?>> extract, MetadataKey key)
    {
        MetadataValue<?> vBefore = extract.apply(before);
        MetadataValue<?> vAfter = extract.apply(after);

        if (!vBefore.equals(vAfter))
            builder.add(key);
    }
}
