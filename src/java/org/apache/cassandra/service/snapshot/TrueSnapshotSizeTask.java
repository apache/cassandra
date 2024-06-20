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

package org.apache.cassandra.service.snapshot;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

public class TrueSnapshotSizeTask implements Callable<Long>
{
    private final SnapshotManager snapshotManager;
    private final Predicate<TableSnapshot> predicate;

    public TrueSnapshotSizeTask(SnapshotManager snapshotManager, Predicate<TableSnapshot> predicate)
    {
        this.snapshotManager = snapshotManager;
        this.predicate = predicate;
    }

    @Override
    public Long call()
    {
        long size = 0;
        for (TableSnapshot snapshot : snapshotManager.getSnapshots())
        {
            if (predicate.test(snapshot))
                try
                {
                    size += snapshot.computeTrueSizeBytes(getTablesFiles(snapshot.getKeyspaceName(), snapshot.getTableName()));
                }
                catch (Throwable ex)
                {
                    // if any error happens while computing size, we don't include
                    return 0L;
                }
        }

        return size;
    }

    private Set<String> getTablesFiles(String keyspaceName, String tableName)
    {
        try
        {
            Keyspace keyspace = Keyspace.getValidKeyspace(keyspaceName);
            ColumnFamilyStore table = keyspace.getColumnFamilyStore(tableName);

            return table.getFilesOfCfs();
        }
        catch (IllegalArgumentException ex)
        {
            // keyspace / table not found
            return null;
        }
    }

}
