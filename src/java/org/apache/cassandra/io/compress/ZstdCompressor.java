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

package org.apache.cassandra.io.compress;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ZSTD Compressor
 */
public class ZstdCompressor extends ZstdCompressorBase implements ICompressor
{
    private static final ConcurrentHashMap<Integer, ZstdCompressor> instances = new ConcurrentHashMap<>();

    /**
     * Create a Zstd compressor with the given options
     * Invoked by {@link org.apache.cassandra.schema.CompressionParams#createCompressor} via reflection
     *
     * @param options compression options
     * @return ZstdCompressor
     */
    public static ZstdCompressor create(Map<String, String> options)
    {
        int level = getOrDefaultCompressionLevel(options);
        validateCompressionLevel(level);
        return getOrCreate(level);
    }

    /**
     * Get a cached instance or return a new one
     *
     * @param level
     * @return
     */
    public static ZstdCompressor getOrCreate(int level)
    {
        return instances.computeIfAbsent(level, ZstdCompressor::new);
    }

    /**
     * Private constructor
     *
     * @param compressionLevel
     */
    private ZstdCompressor(int compressionLevel)
    {
        super(compressionLevel, Collections.singleton(COMPRESSION_LEVEL_OPTION_NAME));
    }
}
