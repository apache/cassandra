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

package org.apache.cassandra.io.sstable;

import java.util.Set;

import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_SSTABLE_WATCHER;

/**
 * Watcher used when opening sstables to discover extra components, eg. archive component
 */
public interface SSTableWatcher
{
    SSTableWatcher instance = !CUSTOM_SSTABLE_WATCHER.isPresent()
                               ? new SSTableWatcher() {}
                               : FBUtilities.construct(CUSTOM_SSTABLE_WATCHER.getString(), "sstable watcher");

    /**
     * Discover extra components before reading TOC file
     *
     * @param descriptor sstable descriptor for current sstable
     */
    default void discoverComponents(Descriptor descriptor)
    {
    }

    /**
     * Discover extra components before opening sstable
     *
     * @param descriptor sstable descriptor for current sstable
     * @param existing existing sstable components
     * @return all discovered sstable components
     */
    default Set<Component> discoverComponents(Descriptor descriptor, Set<Component> existing)
    {
        return existing;
    }
}
