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

import java.util.function.Function;

/**
 * Configuration settings to disable some features in C* that are normally
 * not required or not supported on custom file system.
 *
 * List of configs to be disable for remote FS:
 * - {@link Config#index_summary_resize_interval_in_minutes} = -1
 * - {@link Config#netty_zerocopy_enabled} = false
 * - {@link DatabaseDescriptor#setCommitLogSegmentMgrProvider(Function)} to append host id to commitlog path
 */
public class StorageFlagsConfig
{
    public boolean supports_blacklisting_directory = true;

    public boolean supports_sstable_read_meter = true;

    public boolean supports_hardlinks_for_entire_sstable_streaming = true;

    public boolean supports_flush_before_streaming = true;
}
