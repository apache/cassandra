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

package org.apache.cassandra.distributed.test;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.service.ActiveRepairService;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SnapshotTest extends TestBaseImpl
{
    /**
     * This tests that creating forced snapshot when using multiple data directories
     * will update always the same manifest file, not creating a new one.
     *
     * @throws Throwable
     */
    @Test
    public void testForcedSnapshot() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(3) // 3 dirs to dispers SSTables among different dirs
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk uuid primary key)");

            cluster.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> {
                Keyspace.open("distributed_test_keyspace").getColumnFamilyStore("tbl").disableAutoCompaction();
            });

            createSSTables(cluster);
            takeForcedSnapshot(cluster);
            File manifest1 = getManifest(cluster);
            Set<String> ssTablesFromManifest1 = getSSTablesFromManifest(manifest1);

            createSSTables(cluster);
            takeForcedSnapshot(cluster);
            File manifest2 = getManifest(cluster);
            Set<String> ssTablesFromManifest2 = getSSTablesFromManifest(manifest1);

            assertEquals(manifest1, manifest2);
            assertTrue(ssTablesFromManifest1.size() < ssTablesFromManifest2.size());
            assertTrue(ssTablesFromManifest2.containsAll(ssTablesFromManifest1));
        }
    }

    private Set<String> getSSTablesFromManifest(File manifest) throws Throwable
    {
        JSONObject jsonObject = (JSONObject) new JSONParser().parse(new FileReader(manifest));

        Set<String> sstables = new HashSet<>();

        Object filesObject = jsonObject.get("files");
        if (filesObject instanceof JSONArray)
        {
            JSONArray oldFiles = (JSONArray) filesObject;
            ListIterator listIterator = oldFiles.listIterator();
            while (listIterator.hasNext())
            {
                String next = (String) listIterator.next();
                sstables.add(next);
            }
        }

        return sstables;
    }

    private void createSSTables(Cluster cluster)
    {
        for (int i = 0; i < 10; i++)
        {
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk) values (?)", UUID.randomUUID());
            cluster.get(1).flush(KEYSPACE);
        }
    }

    private File getManifest(Cluster cluster)
    {
        String manifestFileName = cluster.get(1).callOnInstance((IIsolatedExecutor.SerializableCallable<String>) () -> {
            ColumnFamilyStore cfs = Keyspace.open("distributed_test_keyspace").getColumnFamilyStore("tbl");

            List<File> allManifests = new ArrayList<>();
            for (File file : cfs.getDirectories().getSnapshotDirsWithoutCreation("a_snapshot"))
            {
                File maybeManifest = new File(file, "manifest.json");
                if (maybeManifest.exists())
                    allManifests.add(maybeManifest);
            }

            assertEquals(1, allManifests.size());
            return allManifests.get(0).getAbsolutePath();
        });
        return new File(manifestFileName);
    }

    private void takeForcedSnapshot(Cluster cluster)
    {
        cluster.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> {
            try
            {
                ActiveRepairService.instance.snapshotExecutor.submit(() -> {
                    Keyspace.open("distributed_test_keyspace")
                            .getColumnFamilyStore("tbl")
                            .snapshot("a_snapshot", sstable -> true, true, false);
                }).get();
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        });
    }
}
