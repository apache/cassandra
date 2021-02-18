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

import com.google.common.base.Objects;

import org.apache.cassandra.exceptions.ConfigurationException;

public class StorageAttachedIndexOptions
{
    public static final int DEFAULT_SEGMENT_BUFFER_MB = 1024;
    public static final double DEFAULT_ZEROCOPY_USED_THRESHOLD = 0.3;

    private static final int MAXIMUM_SEGMENT_BUFFER_MB = 32768;

    public int segment_write_buffer_space_mb = DEFAULT_SEGMENT_BUFFER_MB;
    public double zerocopy_used_threshold = DEFAULT_ZEROCOPY_USED_THRESHOLD;

    public void validate()
    {
        if ((segment_write_buffer_space_mb < 0) || (segment_write_buffer_space_mb > MAXIMUM_SEGMENT_BUFFER_MB))
        {
            throw new ConfigurationException("Invalid value for segment_write_buffer_space_mb. " +
                                             "Value must be a positive integer less than 32768");
        }

        if ((zerocopy_used_threshold < 0.0) || (zerocopy_used_threshold > 1.0))
        {
            throw new ConfigurationException("Invalid value for zero_copy_used_threshold. " +
                                             "Value must be between 0.0 and 1.0");
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StorageAttachedIndexOptions that = (StorageAttachedIndexOptions) o;
        return Objects.equal(segment_write_buffer_space_mb, that.segment_write_buffer_space_mb) &&
               Objects.equal(zerocopy_used_threshold, that.zerocopy_used_threshold);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(segment_write_buffer_space_mb, zerocopy_used_threshold);
    }
}
