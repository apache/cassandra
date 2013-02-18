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
package org.apache.cassandra.db.compaction;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This class was added to be able to migrate pre-CASSANDRA-4782 leveled manifests into the sstable metadata
 *
 * @deprecated since it can be removed in a future revision.
 */
@Deprecated
public class LegacyLeveledManifest
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyLeveledManifest.class);

    private Map<Integer, Integer> sstableLevels;

    private LegacyLeveledManifest(File path) throws IOException
    {
        sstableLevels = new HashMap<Integer, Integer>();
        ObjectMapper m = new ObjectMapper();
        JsonNode rootNode = m.readValue(path, JsonNode.class);
        JsonNode generations = rootNode.get("generations");
        assert generations.isArray();
        for (JsonNode generation : generations)
        {
            int level = generation.get("generation").getIntValue();
            JsonNode generationValues = generation.get("members");
            for (JsonNode generationValue : generationValues)
            {
                sstableLevels.put(generationValue.getIntValue(), level);
            }
        }
    }

    private int levelOf(int sstableGeneration)
    {
        return sstableLevels.containsKey(sstableGeneration) ? sstableLevels.get(sstableGeneration) : 0;
    }

    /**
     * We need to migrate if there is a legacy leveledmanifest json-file
     * <p/>
     * If there is no jsonfile, we can just start as normally, sstable level will be at 0 for all tables.
     *
     * @param keyspace
     * @param columnFamily
     * @return
     */
    public static boolean manifestNeedsMigration(String keyspace, String columnFamily)
    {
        return Directories.create(keyspace, columnFamily).tryGetLeveledManifest() != null;
    }

    public static void migrateManifests(String keyspace, String columnFamily) throws IOException
    {
        logger.info("Migrating manifest for {}/{}", keyspace, columnFamily);

        snapshotWithoutCFS(keyspace, columnFamily);
        Directories directories = Directories.create(keyspace, columnFamily);
        File manifestFile = directories.tryGetLeveledManifest();
        if (manifestFile == null)
            return;

        LegacyLeveledManifest legacyManifest = new LegacyLeveledManifest(manifestFile);
        for (Map.Entry<Descriptor, Set<Component>> entry : directories.sstableLister().includeBackups(false).skipTemporary(true).list().entrySet())
        {
            Descriptor d = entry.getKey();
            SSTableMetadata oldMetadata = SSTableMetadata.serializer.deserialize(d, false);
            String metadataFilename = d.filenameFor(Component.STATS);
            LeveledManifest.mutateLevel(oldMetadata, d, metadataFilename, legacyManifest.levelOf(d.generation));
        }
        FileUtils.deleteWithConfirm(manifestFile);
    }

    /**
     * Snapshot a CF without having to load the sstables in that directory
     *
     * @param keyspace
     * @param columnFamily
     * @throws IOException
     */
    public static void snapshotWithoutCFS(String keyspace, String columnFamily) throws IOException
    {
        Directories directories = Directories.create(keyspace, columnFamily);
        String snapshotName = "pre-sstablemetamigration";
        logger.info("Snapshotting {}, {} to {}", keyspace, columnFamily, snapshotName);

        for (Map.Entry<Descriptor, Set<Component>> entry : directories.sstableLister().includeBackups(false).skipTemporary(true).list().entrySet())
        {
            Descriptor descriptor = entry.getKey();
            File snapshotDirectoryPath = Directories.getSnapshotDirectory(descriptor, snapshotName);
            for (Component component : entry.getValue())
            {
                File sourceFile = new File(descriptor.filenameFor(component));
                File targetLink = new File(snapshotDirectoryPath, sourceFile.getName());
                FileUtils.createHardLink(sourceFile, targetLink);
            }
        }

        File manifestFile = directories.tryGetLeveledManifest();
        if (manifestFile != null)
        {
            File snapshotDirectory = new File(new File(manifestFile.getParentFile(), Directories.SNAPSHOT_SUBDIR), snapshotName);
            File target = new File(snapshotDirectory, manifestFile.getName());
            FileUtils.createHardLink(manifestFile, target);
        }
    }
}
