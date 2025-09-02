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

import java.io.IOException;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.replication.ImmutableCoordinatorLogOffsets;
import org.apache.cassandra.utils.TimeUUID;

public class RangeAwarePendingSSTableWriter extends RangeAwareSSTableWriter
{
    private final TimeUUID planId;

    public RangeAwarePendingSSTableWriter(ColumnFamilyStore cfs, long estimatedKeys, long repairedAt, TimeUUID pendingRepair, ImmutableCoordinatorLogOffsets coordinatorLogOffsets, SSTableFormat<?, ?> format, int sstableLevel, long totalSize, ILifecycleTransaction txn, SerializationHeader header, TimeUUID planId) throws IOException
    {
        super(cfs, estimatedKeys, repairedAt, pendingRepair, coordinatorLogOffsets, format, sstableLevel, totalSize, txn, header);
        this.planId = planId;
    }

    @Override
    protected File currentFile()
    {
        return cfs.getDirectories().getPendingLocationForDisk(directories.get(currentIndex), planId);
    }
}
