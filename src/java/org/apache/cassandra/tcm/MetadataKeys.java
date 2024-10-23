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

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.ExtensionValue;

import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

public class MetadataKeys
{
    public static final String CORE_NS = toLowerCaseLocalized(MetadataKeys.class.getPackage().getName(), Locale.ROOT);

    public static final MetadataKey SCHEMA                  = make(CORE_NS, "schema", "dist_schema");
    public static final MetadataKey NODE_DIRECTORY          = make(CORE_NS, "membership", "node_directory");
    public static final MetadataKey TOKEN_MAP               = make(CORE_NS, "ownership", "token_map");
    public static final MetadataKey DATA_PLACEMENTS         = make(CORE_NS, "ownership", "data_placements");
    public static final MetadataKey LOCKED_RANGES           = make(CORE_NS, "sequences", "locked_ranges");
    public static final MetadataKey IN_PROGRESS_SEQUENCES   = make(CORE_NS, "sequences", "in_progress");

    public static final ImmutableSet<MetadataKey> CORE_METADATA = ImmutableSet.of(SCHEMA,
                                                                                  NODE_DIRECTORY,
                                                                                  TOKEN_MAP,
                                                                                  DATA_PLACEMENTS,
                                                                                  LOCKED_RANGES,
                                                                                  IN_PROGRESS_SEQUENCES);

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

    public static ImmutableSet<MetadataKey> diffKeys(ClusterMetadata before, ClusterMetadata after)
    {
        ImmutableSet.Builder<MetadataKey> builder = new ImmutableSet.Builder<>();
        diffKeys(before, after, builder);
        return builder.build();
    }

    private static void diffKeys(ClusterMetadata before, ClusterMetadata after, ImmutableSet.Builder<MetadataKey> builder)
    {
        checkKey(before, after, builder, cm -> cm.schema, MetadataKeys.SCHEMA);
        checkKey(before, after, builder, cm -> cm.directory, MetadataKeys.NODE_DIRECTORY);
        checkKey(before, after, builder, cm -> cm.tokenMap, MetadataKeys.TOKEN_MAP);
        checkKey(before, after, builder, cm -> cm.placements, MetadataKeys.DATA_PLACEMENTS);
        checkKey(before, after, builder, cm -> cm.lockedRanges, MetadataKeys.LOCKED_RANGES);
        checkKey(before, after, builder, cm -> cm.inProgressSequences, MetadataKeys.IN_PROGRESS_SEQUENCES);

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
