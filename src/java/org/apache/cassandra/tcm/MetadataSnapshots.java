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

package org.apache.cassandra.tcm;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;

/**
 * {@code MetadataSnapshots} allow to store and retrieve cluster metadata snapshots.
 * Snapshots are optimizations used to make local startup quicker or allow faster catch up by avoiding having to replay
 * the all transformation history.
 */
public interface MetadataSnapshots
{
    Logger logger = LoggerFactory.getLogger(MetadataSnapshots.class);

    ClusterMetadata getLatestSnapshotAfter(Epoch epoch);

    /**
     * Retrieves the cluster metadata snapshot taken at the specified epoch.
     *
     * @param epoch the epoch for which the snapshot must be retrieved
     * @return the cluster metadata snapshot for the specified epoch or {@code null} if no snapshot exists for the epoch.
     */
    ClusterMetadata getSnapshot(Epoch epoch);

    /**
     * Store the specified snapshot
     * @param metadata the cluster metadata snapshot
     */
    void storeSnapshot(ClusterMetadata metadata);

    MetadataSnapshots NO_OP = new MetadataSnapshots()
    {
        @Override
        public ClusterMetadata getLatestSnapshotAfter(Epoch epoch)
        {
            return null;
        }

        @Override
        public ClusterMetadata getSnapshot(Epoch epoch)
        {
            return null;
        }

        @Override
        public void storeSnapshot(ClusterMetadata metadata) {}
    };

    class SystemKeyspaceMetadataSnapshots implements MetadataSnapshots
    {
        @Override
        public ClusterMetadata getLatestSnapshotAfter(Epoch epoch)
        {
            Sealed sealed = Sealed.lookupForSnapshot(epoch);
            return sealed.epoch.isAfter(epoch) ? getSnapshot(sealed.epoch) : null;
        }

        @Override
        public ClusterMetadata getSnapshot(Epoch epoch)
        {
            try
            {
                return ClusterMetadata.fromBytes(SystemKeyspace.getSnapshot(epoch));
            }
            catch (IOException e)
            {
                logger.error("Could not load snapshot", e);
                return null;
            }
        }

        @Override
        public void storeSnapshot(ClusterMetadata metadata)
        {
            try
            {
                SystemKeyspace.storeSnapshot(metadata.epoch, metadata.period, ClusterMetadata.toBytes(metadata));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
