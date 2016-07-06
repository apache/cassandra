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
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;

import org.apache.cassandra.Util;
import static org.junit.Assert.assertEquals;

public class SSTableUtils
{
    // first configured keyspace and cf
    public static String KEYSPACENAME = "Keyspace1";
    public static String CFNAME = "Standard1";

    public SSTableUtils(String ksname, String cfname)
    {
        KEYSPACENAME = ksname;
        CFNAME = cfname;
    }

    /*
    public static ColumnFamily createCF(long mfda, int ldt, Cell... cols)
    {
        return createCF(KEYSPACENAME, CFNAME, mfda, ldt, cols);
    }

    public static ColumnFamily createCF(String ksname, String cfname, long mfda, int ldt, Cell... cols)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(ksname, cfname);
        cf.delete(new DeletionInfo(mfda, ldt));
        for (Cell col : cols)
            cf.addColumn(col);
        return cf;
    }

    public static File tempSSTableFile(String keyspaceName, String cfname) throws IOException
    {
        return tempSSTableFile(keyspaceName, cfname, 0);
    }
    */

    public static File tempSSTableFile(String keyspaceName, String cfname, int generation) throws IOException
    {
        File tempdir = File.createTempFile(keyspaceName, cfname);
        if(!tempdir.delete() || !tempdir.mkdir())
            throw new IOException("Temporary directory creation failed.");
        tempdir.deleteOnExit();
        File cfDir = new File(tempdir, keyspaceName + File.separator + cfname);
        cfDir.mkdirs();
        cfDir.deleteOnExit();
        File datafile = new File(new Descriptor(cfDir, keyspaceName, cfname, generation, SSTableFormat.Type.BIG).filenameFor(Component.DATA));
        if (!datafile.createNewFile())
            throw new IOException("unable to create file " + datafile);
        datafile.deleteOnExit();
        return datafile;
    }

    public static void assertContentEquals(SSTableReader lhs, SSTableReader rhs) throws Exception
    {
        try (ISSTableScanner slhs = lhs.getScanner();
             ISSTableScanner srhs = rhs.getScanner())
        {
            while (slhs.hasNext())
            {
                UnfilteredRowIterator ilhs = slhs.next();
                assert srhs.hasNext() : "LHS contained more rows than RHS";
                UnfilteredRowIterator irhs = srhs.next();
                assertContentEquals(ilhs, irhs);
            }
            assert !srhs.hasNext() : "RHS contained more rows than LHS";
        }
    }

    public static void assertContentEquals(UnfilteredRowIterator lhs, UnfilteredRowIterator rhs)
    {
        assertEquals(lhs.partitionKey(), rhs.partitionKey());
        assertEquals(lhs.partitionLevelDeletion(), rhs.partitionLevelDeletion());
        // iterate columns
        while (lhs.hasNext())
        {
            Unfiltered clhs = lhs.next();
            assert rhs.hasNext() : "LHS contained more columns than RHS for " + lhs.partitionKey();
            Unfiltered crhs = rhs.next();

            assertEquals("Mismatched row/tombstone for " + lhs.partitionKey(), clhs, crhs);
        }
        assert !rhs.hasNext() : "RHS contained more columns than LHS for " + lhs.partitionKey();
    }

    /**
     * @return A Context with chainable methods to configure and write a SSTable.
     */
    public static Context prepare()
    {
        return new Context();
    }

    public static class Context
    {
        private String ksname = KEYSPACENAME;
        private String cfname = CFNAME;
        private Descriptor dest = null;
        private boolean cleanup = true;
        private int generation = 0;

        Context() {}

        public Context ks(String ksname)
        {
            this.ksname = ksname;
            return this;
        }

        public Context cf(String cfname)
        {
            this.cfname = cfname;
            return this;
        }

        /**
         * Set an alternate path for the written SSTable: if unset, the SSTable will
         * be cleaned up on JVM exit.
         */
        public Context dest(Descriptor dest)
        {
            this.dest = dest;
            this.cleanup = false;
            return this;
        }

        /**
         * Sets the generation number for the generated SSTable. Ignored if "dest()" is set.
         */
        public Context generation(int generation)
        {
            this.generation = generation;
            return this;
        }

        public Collection<SSTableReader> write(Set<String> keys) throws IOException
        {
            Map<String, PartitionUpdate> map = new HashMap<>();
            for (String key : keys)
            {
                RowUpdateBuilder builder = new RowUpdateBuilder(Schema.instance.getCFMetaData(ksname, cfname), 0, key);
                builder.clustering(key).add("val", key);
                map.put(key, builder.buildUpdate());
            }
            return write(map);
        }

        public Collection<SSTableReader> write(SortedMap<DecoratedKey, PartitionUpdate> sorted) throws IOException
        {
            PartitionColumns.Builder builder = PartitionColumns.builder();
            for (PartitionUpdate update : sorted.values())
                builder.addAll(update.columns());
            final Iterator<Map.Entry<DecoratedKey, PartitionUpdate>> iter = sorted.entrySet().iterator();
            return write(sorted.size(), new Appender()
            {
                public SerializationHeader header()
                {
                    return new SerializationHeader(true, Schema.instance.getCFMetaData(ksname, cfname), builder.build(), EncodingStats.NO_STATS);
                }

                @Override
                public boolean append(SSTableTxnWriter writer) throws IOException
                {
                    if (!iter.hasNext())
                        return false;
                    writer.append(iter.next().getValue().unfilteredIterator());
                    return true;
                }
            });
        }

        public Collection<SSTableReader> write(Map<String, PartitionUpdate> entries) throws IOException
        {
            SortedMap<DecoratedKey, PartitionUpdate> sorted = new TreeMap<>();
            for (Map.Entry<String, PartitionUpdate> entry : entries.entrySet())
                sorted.put(Util.dk(entry.getKey()), entry.getValue());

            return write(sorted);
        }

        public Collection<SSTableReader> write(int expectedSize, Appender appender) throws IOException
        {
            File datafile = (dest == null) ? tempSSTableFile(ksname, cfname, generation) : new File(dest.filenameFor(Component.DATA));
            CFMetaData cfm = Schema.instance.getCFMetaData(ksname, cfname);
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.cfId);
            SerializationHeader header = appender.header();
            SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, datafile.getAbsolutePath(), expectedSize, ActiveRepairService.UNREPAIRED_SSTABLE, 0, header);
            while (appender.append(writer)) { /* pass */ }
            Collection<SSTableReader> readers = writer.finish(true);

            // mark all components for removal
            if (cleanup)
                for (SSTableReader reader: readers)
                    for (Component component : reader.components)
                        new File(reader.descriptor.filenameFor(component)).deleteOnExit();
            return readers;
        }
    }

    public static abstract class Appender
    {
        public abstract SerializationHeader header();
        /** Called with an open writer until it returns false. */
        public abstract boolean append(SSTableTxnWriter writer) throws IOException;
    }
}
