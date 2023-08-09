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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.exceptions.ConfigurationException;

public class StorageAttachedIndexOptions
{
    public static final int DEFAULT_SEGMENT_BUFFER_MB = 1024;

    @VisibleForTesting
    public static final int MAXIMUM_SEGMENT_BUFFER_MB = 32768;

    @VisibleForTesting
    public static final String INVALID_BUFFER_SIZE_ERROR = "Invalid value for segment_write_buffer_size. " +
                                                           "Value must be a positive integer less than " + MAXIMUM_SEGMENT_BUFFER_MB + "MiB";

    public DataStorageSpec.IntMebibytesBound segment_write_buffer_size = new DataStorageSpec.IntMebibytesBound(DEFAULT_SEGMENT_BUFFER_MB);

    public void validate()
    {
        if (segment_write_buffer_size.toMebibytes() > MAXIMUM_SEGMENT_BUFFER_MB)
        {
            throw new ConfigurationException(INVALID_BUFFER_SIZE_ERROR);
        }
    }
}
