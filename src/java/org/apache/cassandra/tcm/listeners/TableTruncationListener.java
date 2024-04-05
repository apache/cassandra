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

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.Truncations.TruncationRecord;

public class TableTruncationListener implements ChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(TableTruncationListener.class);

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        ImmutableMap<TableId, TruncationRecord> diff = prev.truncations.diff(next.truncations);

        for (Map.Entry<TableId, TruncationRecord> entry : diff.entrySet())
            truncate(entry.getKey(), entry.getValue());
    }

    private void truncate(TableId tableId, TruncationRecord truncationRecord)
    {
        TableMetadata tableOrViewNullable = ClusterMetadata.current().schema.getKeyspaces().getTableOrViewNullable(tableId);
        if (tableOrViewNullable == null)
            return;

        ColumnFamilyStore columnFamilyStore = Keyspace.openAndGetStore(tableOrViewNullable);

        long truncatedAt = SystemKeyspace.getTruncatedAt(tableId);

        if (truncationRecord.truncationTimestamp > truncatedAt)
        {
            columnFamilyStore.truncateBlocking(truncationRecord.truncationTimestamp);
        }
    }
}
