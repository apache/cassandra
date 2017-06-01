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
package org.apache.cassandra.io.sstable.format;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.io.File;
import java.nio.ByteBuffer;

import com.google.common.util.concurrent.Runnables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;

/**
 * Tests backwards compatibility for SSTables
 */
public class ClientModeSSTableTest
{
    public static final String LEGACY_SSTABLE_PROP = "legacy-sstable-root";
    public static final String KSNAME = "Keyspace1";
    public static final String CFNAME = "Standard1";

    public static File LEGACY_SSTABLE_ROOT;

    static CFMetaData metadata;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.toolInitialization();

        metadata = CFMetaData.Builder.createDense(KSNAME, CFNAME, false, false)
                                                .addPartitionKey("key", BytesType.instance)
                                                .addClusteringColumn("column", BytesType.instance)
                                                .addRegularColumn("value", BytesType.instance)
                                                .withPartitioner(ByteOrderedPartitioner.instance)
                                                .build();

        String scp = System.getProperty(LEGACY_SSTABLE_PROP);
        assert scp != null;
        LEGACY_SSTABLE_ROOT = new File(scp).getAbsoluteFile();
        assert LEGACY_SSTABLE_ROOT.isDirectory();
    }

    /**
     * Get a descriptor for the legacy sstable at the given version.
     */
    protected Descriptor getDescriptor(String ver)
    {
        File directory = new File(LEGACY_SSTABLE_ROOT + File.separator + ver + File.separator + KSNAME);
        return new Descriptor(ver, directory, KSNAME, CFNAME, 0, SSTableFormat.Type.LEGACY);
    }

    @Test
    public void testVersions() throws Throwable
    {
        boolean notSkipped = false;

        for (File version : LEGACY_SSTABLE_ROOT.listFiles())
        {
            if (!new File(LEGACY_SSTABLE_ROOT + File.separator + version.getName() + File.separator + KSNAME).isDirectory())
                continue;
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
        SSTableReader reader = null;
        try
        {
            reader = SSTableReader.openNoValidation(getDescriptor(version), metadata);

            ByteBuffer key = bytes(Integer.toString(100));

            try (UnfilteredRowIterator iter = reader.iterator(metadata.decorateKey(key),
                                                              Slices.ALL,
                                                              ColumnFilter.selection(metadata.partitionColumns()),
                                                              false,
                                                              false,
                                                              SSTableReadsListener.NOOP_LISTENER))
            {
                assert iter.next().clustering().get(0).equals(key);
            }
        }
        catch (Throwable e)
        {
            System.err.println("Failed to read " + version);
            throw e;
        }
        finally
        {
            if (reader != null)
            {
                int globalTidyCount = SSTableReader.GlobalTidy.lookup.size();
                reader.selfRef().release();
                assert reader.selfRef().globalCount() == 0;

                // await clean-up to complete if started.
                ScheduledExecutors.nonPeriodicTasks.submit(Runnables.doNothing()).get();
                // Ensure clean-up completed.
                assert SSTableReader.GlobalTidy.lookup.size() < globalTidyCount;
            }
        }
    }
}
