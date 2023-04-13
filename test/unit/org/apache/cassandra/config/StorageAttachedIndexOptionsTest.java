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

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StorageAttachedIndexOptionsTest
{
    @Test
    public void testStorageAttachedIndexOptionsValidation()
    {
        StorageAttachedIndexOptions saiOptions = new StorageAttachedIndexOptions();

        saiOptions.segment_write_buffer_size = new DataStorageSpec.IntMebibytesBound(0);
        saiOptions.validate();

        saiOptions.segment_write_buffer_size = new DataStorageSpec.IntMebibytesBound(StorageAttachedIndexOptions.MAXIMUM_SEGMENT_BUFFER_MB);
        saiOptions.validate();

        saiOptions.segment_write_buffer_size = new DataStorageSpec.IntMebibytesBound(StorageAttachedIndexOptions.MAXIMUM_SEGMENT_BUFFER_MB + 1);
        assertThatThrownBy(saiOptions::validate).isInstanceOf(ConfigurationException.class)
                                                .hasMessage(StorageAttachedIndexOptions.INVALID_BUFFER_SIZE_ERROR);
    }
}
