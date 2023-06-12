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

package org.apache.cassandra.utils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;

/**
 * The mode of compatibility with older Cassandra versions.
 */
public enum StorageCompatibilityMode
{
    /**
     * Stay compatible with Cassandra 4.x, opting out from any new features that will prevent a rollback to 4.x.
     * At the moment, this means:
     *  - Deletion times will be limited to 2038, instead of the year 2106 limit introduced by 5.0.
     */
    CASSANDRA_4(4),

    /**
     * Use the storage formats of the current version, but disabling features that are not compatible with any
     * not-upgraded nodes in the cluster. Use this during rolling upgrades to a new major Cassandra version. Once all
     * nodes have been upgraded, you can set the compatibility to {@link #NONE}.
     */
    UPGRADING(Integer.MAX_VALUE - 1),

    /**
     * Don't try to be compatible with older versions. Data will be written with the most recent format, which might
     * prevent a rollback to previous Cassandra versions. Features that are not compatible with older nodes will be
     * enabled, assuming that all nodes in the cluster are in the same major version as this node.
     */
    NONE(Integer.MAX_VALUE);

    public final int major;

    StorageCompatibilityMode(int major)
    {
        this.major = major;
    }

    public static StorageCompatibilityMode current()
    {
        return DatabaseDescriptor.getStorageCompatibilityMode();
    }

    public boolean disabled()
    {
        return this == NONE;
    }

    public boolean isBefore(int major)
    {
        return this.major < major;
    }

    public void validateSstableFormat(SSTableFormat<?, ?> selectedFormat)
    {
        if (selectedFormat.name().equals(BtiFormat.NAME) && this == StorageCompatibilityMode.CASSANDRA_4)
            throw new ConfigurationException(String.format("Selected sstable format '%s' is not available when in storage compatibility mode '%s'.",
                                                           selectedFormat.name(),
                                                           this));
    }
}
