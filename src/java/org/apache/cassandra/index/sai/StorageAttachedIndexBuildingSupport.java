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

package org.apache.cassandra.index.sai;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;

class StorageAttachedIndexBuildingSupport implements Index.IndexBuildingSupport
{
    @Override
    public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs,
                                                   Set<Index> indexes,
                                                   Collection<SSTableReader> sstablesToRebuild,
                                                   boolean isFullRebuild)
    {
        NavigableMap<SSTableReader, Set<StorageAttachedIndex>> sstables = new TreeMap<>(Comparator.comparing(s -> s.descriptor.id, SSTableIdFactory.COMPARATOR));
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);

        assert group != null : "Index group does not exist for table " + cfs.keyspace + '.' + cfs.name;

        indexes.stream()
               .filter((i) -> i instanceof StorageAttachedIndex)
               .forEach((i) ->
                        {
                            StorageAttachedIndex sai = (StorageAttachedIndex) i;

                            // If this is not a full manual index rebuild we can skip SSTables that already have an
                            // attached index. Otherwise, we override any pre-existent index.
                            Collection<SSTableReader> ss = sstablesToRebuild;
                            if (!isFullRebuild)
                            {
                                ss = sstablesToRebuild.stream()
                                                      .filter(s -> !IndexDescriptor.create(s).isPerColumnIndexBuildComplete(sai.identifier()))
                                                      .collect(Collectors.toList());
                            }

                            group.dropIndexSSTables(ss, sai);

                            ss.forEach(sstable -> sstables.computeIfAbsent(sstable, ignore -> new HashSet<>()).add(sai));
                        });

        return new StorageAttachedIndexBuilder(group, sstables, isFullRebuild, false);
    }
}
