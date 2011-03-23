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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.columniterator.SSTableNamesIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests backwards compatibility for SSTables. Requires that older SSTables match up with the existing config file,
 * and currently only tests specific cases for specific upgrades.
 */
public class LegacySSTableTest extends CleanupHelper
{
    public static final String LEGACY_SSTABLE_PROP = "legacy-sstable-root";
    public static final String KSNAME = "Keyspace1";
    public static final String CFNAME = "Standard1";

    public static Set<String> TEST_DATA;
    public static File LEGACY_SSTABLE_ROOT;

    @BeforeClass
    public static void beforeClass()
    {
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
    protected Descriptor getDescriptor(String ver) throws IOException
    {
        File directory = new File(LEGACY_SSTABLE_ROOT + File.separator + ver + File.separator + KSNAME);
        return new Descriptor(ver, directory, KSNAME, CFNAME, 0, false);
    }

    /**
     * Generates a test SSTable for use in this classes' tests. Uncomment and run against an older build
     * and the output will be copied to a version subdirectory in 'LEGACY_SSTABLE_ROOT'
     *
    @Test
    public void buildTestSSTable() throws IOException
    {
        // write the output in a version specific directory
        Descriptor dest = getDescriptor(Descriptor.CURRENT_VERSION);
        assert dest.directory.mkdirs() : "Could not create " + dest.directory + ". Might it already exist?";

        SSTableReader ssTable = SSTableUtils.prepare().ks(KSNAME).cf(CFNAME).dest(dest).write(TEST_DATA);
        assert ssTable.descriptor.generation == 0 :
            "In order to create a generation 0 sstable, please run this test alone.";
        System.out.println(">>> Wrote " + dest);
    }
    */

    @Test
    public void testVersions() throws Throwable
    {
        for (File version : LEGACY_SSTABLE_ROOT.listFiles())
            if (Descriptor.versionValidate(version.getName()))
                testVersion(version.getName());
    }

    public void testVersion(String version) throws Throwable
    {
        try
        {
            SSTableReader reader = SSTableReader.open(getDescriptor(version));
            for (String keystring : TEST_DATA)
            {
                ByteBuffer key = ByteBufferUtil.bytes(keystring);
                // confirm that the bloom filter does not reject any keys/names
                DecoratedKey dk = reader.partitioner.decorateKey(key);
                SSTableNamesIterator iter = new SSTableNamesIterator(reader, dk, FBUtilities.singleton(key));
                assert iter.next().name().equals(key);
            }
        }
        catch (Throwable e)
        {
            System.err.println("Failed to read " + version);
            throw e;
        }
    }
}
