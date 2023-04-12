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
import java.util.Map;

import com.google.common.base.Objects;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SSTableCompressionOptions
{
    /**
     * The options necessary to create the default CompressonParams.
     */
    public static final SSTableCompressionOptions DEFAULT = null;
    public String chunk_length=null;
    public String max_compressed_length=null;
    public Double min_compress_ratio = null;
    public boolean enabled = true;
    public String class_name;
    public Map<String, String> parameters;

    public SSTableCompressionOptions()
    {
       // for use by yamlsnake
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof SSTableCompressionOptions && equals((SSTableCompressionOptions) o);
    }

    public boolean equals(SSTableCompressionOptions other)
    {
        return Objects.equal(class_name, other.class_name) &&
               Objects.equal(enabled, other.enabled) &&
               Objects.equal(min_compress_ratio, other.min_compress_ratio) &&
               Objects.equal(max_compressed_length, other.max_compressed_length) &&
               Objects.equal(chunk_length, other.chunk_length) &&
               Objects.equal(parameters, other.parameters);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
               .append(class_name)
               .append(chunk_length)
               .append(min_compress_ratio)
               .append(parameters)
               .toHashCode();
    }

    public enum CompressorType
    {
        lz4,
        none,
        noop,
        snappy,
        deflate,
        zstd,
    }
}
