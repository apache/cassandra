/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.io.sstable;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Tests backwards compatibility for SSTables
 */
public class LegacySSTableTest
{
    public static final String LEGACY_SSTABLE_PROP = "legacy-sstable-root";
    public static final String KSNAME = "Keyspace1";
    public static final String CFNAME = "Standard1";

    public static Set<String> TEST_DATA;
    public static File LEGACY_SSTABLE_ROOT;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KSNAME,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KSNAME, CFNAME));
        beforeClass();
    }

    public static void beforeClass()
    {
        Keyspace.setInitialized();
        String scp = System.getProperty(LEGACY_SSTABLE_PROP);
        assert scp != null;
        LEGACY_SSTABLE_ROOT = new File(scp).getAbsoluteFile();
        assert LEGACY_SSTABLE_ROOT.isDirectory();

        TEST_DATA = new HashSet<String>();
        for (int i = 100; i < 1000; ++i)
            TEST_DATA.add(Integer.toString(i));
    }

    /**
     * Get a descriptor for the legacy sstable at the given version.
     */
    protected Descriptor getDescriptor(String ver)
    {
        File directory = new File(LEGACY_SSTABLE_ROOT + File.separator + ver + File.separator + KSNAME);
        return new Descriptor(ver, directory, KSNAME, CFNAME, 0, Descriptor.Type.FINAL, SSTableFormat.Type.LEGACY);
    }

    /**
     * Generates a test SSTable for use in this classes' tests. Uncomment and run against an older build
     * and the output will be copied to a version subdirectory in 'LEGACY_SSTABLE_ROOT'
     *
    @Test
    public void buildTestSSTable() throws IOException
    {
        // write the output in a version specific directory
        Descriptor dest = getDescriptor(Descriptor.Version.current_version);
        assert dest.directory.mkdirs() : "Could not create " + dest.directory + ". Might it already exist?";

        SSTableReader ssTable = SSTableUtils.prepare().ks(KSNAME).cf(CFNAME).dest(dest).write(TEST_DATA);
        assert ssTable.descriptor.generation == 0 :
            "In order to create a generation 0 sstable, please run this test alone.";
        System.out.println(">>> Wrote " + dest);
    }
    */

    @Test
    public void testStreaming() throws Throwable
    {
        StorageService.instance.initServer();

        for (File version : LEGACY_SSTABLE_ROOT.listFiles())
            if (Version.validate(version.getName()) && SSTableFormat.Type.LEGACY.info.getVersion(version.getName()).isCompatible())
                testStreaming(version.getName());
    }

    private void testStreaming(String version) throws Exception
    {
        SSTableReader sstable = SSTableReader.open(getDescriptor(version));
        IPartitioner p = StorageService.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("100"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("100")), p.getMinimumToken()));
        ArrayList<StreamSession.SSTableStreamingSections> details = new ArrayList<>();
        details.add(new StreamSession.SSTableStreamingSections(sstable, sstable.ref(),
                                                               sstable.getPositionsForRanges(ranges),
                                                               sstable.estimatedKeysForRanges(ranges), sstable.getSSTableMetadata().repairedAt));
        new StreamPlan("LegacyStreamingTest").transferFiles(FBUtilities.getBroadcastAddress(), details)
                                             .execute().get();

        ColumnFamilyStore cfs = Keyspace.open(KSNAME).getColumnFamilyStore(CFNAME);
        assert cfs.getSSTables().size() == 1;
        sstable = cfs.getSSTables().iterator().next();
        CellNameType type = sstable.metadata.comparator;
        for (String keystring : TEST_DATA)
        {
            ByteBuffer key = ByteBufferUtil.bytes(keystring);
            OnDiskAtomIterator iter = sstable.iterator(Util.dk(key), FBUtilities.singleton(Util.cellname(key), type));
            ColumnFamily cf = iter.getColumnFamily();

            // check not deleted (CASSANDRA-6527)
            assert cf.deletionInfo().equals(DeletionInfo.live());
            assert iter.next().name().toByteBuffer().equals(key);
        }
        sstable.selfRef().release();
    }

    @Test
    public void testVersions() throws Throwable
    {
        boolean notSkipped = false;

        for (File version : LEGACY_SSTABLE_ROOT.listFiles())
        {
            if (Version.validate(version.getName()) && SSTableFormat.Type.LEGACY.info.getVersion(version.getName()).isCompatible())
            {
                notSkipped = true;
                testVersion(version.getName());
            }
        }

        assert notSkipped;
    }

    public void testVersion(String version) throws Throwable
    {
        try
        {
            SSTableReader reader = SSTableReader.open(getDescriptor(version));
            CellNameType type = reader.metadata.comparator;
            for (String keystring : TEST_DATA)
            {
                ByteBuffer key = ByteBufferUtil.bytes(keystring);
                // confirm that the bloom filter does not reject any keys/names
                DecoratedKey dk = reader.partitioner.decorateKey(key);
                OnDiskAtomIterator iter = reader.iterator(dk, FBUtilities.singleton(Util.cellname(key), type));
                assert iter.next().name().toByteBuffer().equals(key);
            }

            // TODO actually test some reads
        }
        catch (Throwable e)
        {
            System.err.println("Failed to read " + version);
            throw e;
        }
    }
}
