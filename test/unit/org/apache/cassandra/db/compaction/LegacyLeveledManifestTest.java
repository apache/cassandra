package org.apache.cassandra.db.compaction;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.io.util.FileUtils;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LegacyLeveledManifestTest
{
    private File destDir;
    @Before
    public void setup()
    {
        String root = System.getProperty("migration-sstable-root");
        File rootDir = new File(root + File.separator + "hf" + File.separator + "Keyspace1");
        destDir = Directories.create("Keyspace1", "legacyleveled").getDirectoryForNewSSTables(0);
        FileUtils.createDirectory(destDir);
        for (File srcFile : rootDir.listFiles())
        {
            File destFile = new File(destDir, srcFile.getName());
            FileUtils.createHardLink(srcFile,destFile);
            assert destFile.exists() : destFile.getAbsoluteFile();
        }
    }
    @After
    public void tearDown()
    {
        FileUtils.deleteRecursive(destDir);
    }

    @Test
    public void migrateTest() throws IOException
    {
        assertTrue(LegacyLeveledManifest.manifestNeedsMigration("Keyspace1", "legacyleveled"));
    }

    @Test
    public void doMigrationTest() throws IOException, InterruptedException
    {
        LegacyLeveledManifest.migrateManifests("Keyspace1","legacyleveled");

        for (int i = 0; i <= 2; i++)
        {
            Descriptor descriptor = Descriptor.fromFilename(destDir+File.separator+"Keyspace1-legacyleveled-hf-"+i+"-Statistics.db");
            SSTableMetadata metadata = SSTableMetadata.serializer.deserialize(descriptor);
            assertEquals(metadata.sstableLevel, i);
        }
    }

    /**
     * Validate that the rewritten stats file is the same as the original one.
     * @throws IOException
     */
    @Test
    public void validateSSTableMetadataTest() throws IOException
    {
        Map<Descriptor, SSTableMetadata> beforeMigration = new HashMap<Descriptor, SSTableMetadata>();
        for (int i = 0; i <= 2; i++)
        {
            Descriptor descriptor = Descriptor.fromFilename(destDir+File.separator+"Keyspace1-legacyleveled-hf-"+i+"-Statistics.db");
            beforeMigration.put(descriptor, SSTableMetadata.serializer.deserialize(descriptor, false));
        }

        LegacyLeveledManifest.migrateManifests("Keyspace1","legacyleveled");

        for (Map.Entry<Descriptor, SSTableMetadata> entry : beforeMigration.entrySet())
        {
            SSTableMetadata newMetadata = SSTableMetadata.serializer.deserialize(entry.getKey());
            SSTableMetadata oldMetadata = entry.getValue();
            assertEquals(newMetadata.estimatedRowSize, oldMetadata.estimatedRowSize);
            assertEquals(newMetadata.estimatedColumnCount, oldMetadata.estimatedColumnCount);
            assertEquals(newMetadata.replayPosition, oldMetadata.replayPosition);
            assertEquals(newMetadata.minTimestamp, oldMetadata.minTimestamp);
            assertEquals(newMetadata.maxTimestamp, oldMetadata.maxTimestamp);
            assertEquals(newMetadata.compressionRatio, oldMetadata.compressionRatio, 0.01);
            assertEquals(newMetadata.partitioner, oldMetadata.partitioner);
            assertEquals(newMetadata.ancestors, oldMetadata.ancestors);
            assertEquals(newMetadata.estimatedTombstoneDropTime, oldMetadata.estimatedTombstoneDropTime);
        }
    }

}
