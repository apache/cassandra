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

package org.apache.cassandra.tcm.listeners;

import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;

public class TableTruncationListener implements ChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(TableTruncationListener.class);

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        ImmutableMap<UUID, Long> diff = prev.truncations.diff(next.truncations);

        for (Map.Entry<UUID, Long> entry : diff.entrySet())
        {
            TableMetadata tableOrViewNullable = prev.schema.getKeyspaces().getTableOrViewNullable(TableId.fromUUID(entry.getKey()));
            if (tableOrViewNullable == null)
                return;

            ColumnFamilyStore columnFamilyStore = Keyspace.openAndGetStore(tableOrViewNullable);
            long trucatedAt = SystemKeyspace.getTruncatedAt(tableOrViewNullable.id);
            long metadataTimestamp = entry.getValue();
            if (metadataTimestamp > trucatedAt)
                columnFamilyStore.truncateBlocking(metadataTimestamp);
        }
    }
}
