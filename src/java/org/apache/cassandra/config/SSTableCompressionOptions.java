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
package org.apache.cassandra.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.schema.CompressionParams;
import org.checkerframework.checker.units.qual.C;

import static java.lang.String.format;

public class SSTableCompressionOptions
{
    public String chunk_length="";
    public Double min_compress_ratio;
    public boolean enabled = true;
    public Config.CompressorType type;
    public ParameterizedClass compressor;

    public SSTableCompressionOptions()
    {
       // for use by yamlsnake
    }


    public CompressionParams getCompressionParams()
    {
        Config.CompressorType myType = type == null? Config.CompressorType.lz4 : type;
        if (!enabled) {
            myType = Config.CompressorType.none;
        }

        int chunk_length_in_kb = chunk_length.isBlank()
                                 ? CompressionParams.DEFAULT_CHUNK_LENGTH
                                 : new DataStorageSpec.IntKibibytesBound( this.chunk_length ).toKibibytes();

        double min_compress_ratio_as_dbl = this.min_compress_ratio == null || this.min_compress_ratio < 0.0
                                           ? CompressionParams.DEFAULT_MIN_COMPRESS_RATIO : this.min_compress_ratio;

        switch (myType)
        {
            case none:
                return CompressionParams.noCompression();
            default:
            case lz4:
                return CompressionParams.lz4(chunk_length_in_kb);
            case snappy:
               return CompressionParams.snappy(chunk_length_in_kb, min_compress_ratio_as_dbl);
            case deflate:
                return CompressionParams.deflate( chunk_length_in_kb );
            case zstd:
                return CompressionParams.zstd( chunk_length_in_kb );
            case custom:
                if (compressor == null)
                {
                    throw new ConfigurationException("Missing sub-option 'compressor' for the 'sstable_compressor' option with 'custom' type.");
                }
                if (compressor.class_name == null || compressor.class_name.isEmpty())
                {
                    throw new ConfigurationException("Missing or empty sub-option 'class' for the 'sstable_compressor.compressor' option.");
                }
                CompressionParams cp = new CompressionParams(compressor.class_name,
                                             compressor.parameters == null ? Collections.emptyMap() : compressor.parameters,
                                             chunk_length_in_kb,
                                             min_compress_ratio_as_dbl );
                if (cp.getSstableCompressor() == null)
                {
                    throw new ConfigurationException(format("Missing '%s' is not a valid compressor class name.", compressor.class_name));
                }
                return cp;
            case noop:
                return CompressionParams.noop();


        }
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof SSTableCompressionOptions && equals((SSTableCompressionOptions) o);
    }

    public boolean equals(SSTableCompressionOptions other)
    {
        return Objects.equal(type, other.type) &&
               Objects.equal(enabled, other.enabled) &&
               Objects.equal(min_compress_ratio, other.min_compress_ratio) &&
               Objects.equal(chunk_length, other.chunk_length) &&
               Objects.equal(compressor, other.compressor);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
               .append(type.ordinal())
               .append(chunk_length)
               .append(min_compress_ratio)
               .append(compressor)
               .toHashCode();
    }
}
