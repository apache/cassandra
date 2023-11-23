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

import java.util.Locale;

import com.google.common.collect.ImmutableSet;

public class MetadataKeys
{
    public static final String CORE_NS = MetadataKeys.class.getPackage().getName().toLowerCase(Locale.ROOT);

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

}
