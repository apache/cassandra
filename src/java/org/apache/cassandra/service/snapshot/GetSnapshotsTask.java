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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

public class GetSnapshotsTask implements Callable<List<TableSnapshot>>
{
    private final Predicate<TableSnapshot> predicate;
    private final boolean shouldRemoveIfNotExists;

    public GetSnapshotsTask(Predicate<TableSnapshot> predicate, boolean shouldRemoveIfNotExists)
    {
        this.predicate = predicate;
        this.shouldRemoveIfNotExists = shouldRemoveIfNotExists;
    }

    @Override
    public List<TableSnapshot> call() throws Exception
    {
        if (shouldRemoveIfNotExists)
            return getWithRemoval();
        else
            return getWithoutRemoval();
    }

    private List<TableSnapshot> getWithRemoval()
    {
        List<TableSnapshot> notExistingAnymore = new ArrayList<>();
        List<TableSnapshot> snapshots = new ArrayList<>();
        for (TableSnapshot snapshot : SnapshotManager.instance.getSnapshots())
        {
            if (predicate.test(snapshot))
            {
                if (!snapshot.hasManifest())
                    notExistingAnymore.add(snapshot);
                else
                    snapshots.add(snapshot);
            }
        }

        // we do not want to clear snapshots which do not exist periodically
        // because that would beat the purpose of caching (we would need to go to the disk
        // to see if manifests still exists every time), hence, we will clean on listing,
        // we do not need to have cache clean of non-existing snapshots when nobody is looking
        if (!notExistingAnymore.isEmpty())
            notExistingAnymore.forEach(SnapshotManager.instance.getSnapshots()::remove);

        return snapshots;
    }

    private List<TableSnapshot> getWithoutRemoval()
    {
        for (TableSnapshot snapshot : SnapshotManager.instance.getSnapshots())
        {
            if (predicate.test(snapshot))
            {
                if (snapshot.hasManifest())
                    return List.of(snapshot);
            }
        }

        return List.of();
    }
}
