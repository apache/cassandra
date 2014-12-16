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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.cassandra.Util;
import static org.junit.Assert.assertEquals;

public class SSTableUtils
{
    // first configured keyspace and cf
    public static String KEYSPACENAME = "Keyspace1";
    public static String CFNAME = "Standard1";

    public static ColumnFamily createCF(long mfda, int ldt, Cell... cols)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACENAME, CFNAME);
        cf.delete(new DeletionInfo(mfda, ldt));
        for (Cell col : cols)
            cf.addColumn(col);
        return cf;
    }

    public static File tempSSTableFile(String keyspaceName, String cfname) throws IOException
    {
        return tempSSTableFile(keyspaceName, cfname, 0);
    }

    public static File tempSSTableFile(String keyspaceName, String cfname, int generation) throws IOException
    {
        File tempdir = File.createTempFile(keyspaceName, cfname);
        if(!tempdir.delete() || !tempdir.mkdir())
            throw new IOException("Temporary directory creation failed.");
        tempdir.deleteOnExit();
        File keyspaceDir = new File(tempdir, keyspaceName);
        keyspaceDir.mkdir();
        keyspaceDir.deleteOnExit();
        File datafile = new File(new Descriptor(keyspaceDir, keyspaceName, cfname, generation, Descriptor.Type.FINAL).filenameFor("Data.db"));
        if (!datafile.createNewFile())
            throw new IOException("unable to create file " + datafile);
        datafile.deleteOnExit();
        return datafile;
    }

    public static void assertContentEquals(SSTableReader lhs, SSTableReader rhs)
    {
        ISSTableScanner slhs = lhs.getScanner();
        ISSTableScanner srhs = rhs.getScanner();
        while (slhs.hasNext())
        {
            OnDiskAtomIterator ilhs = slhs.next();
            assert srhs.hasNext() : "LHS contained more rows than RHS";
            OnDiskAtomIterator irhs = srhs.next();
            assertContentEquals(ilhs, irhs);
        }
        assert !srhs.hasNext() : "RHS contained more rows than LHS";
    }

    public static void assertContentEquals(OnDiskAtomIterator lhs, OnDiskAtomIterator rhs)
    {
        assertEquals(lhs.getKey(), rhs.getKey());
        // check metadata
        ColumnFamily lcf = lhs.getColumnFamily();
        ColumnFamily rcf = rhs.getColumnFamily();
        if (lcf == null)
        {
            if (rcf == null)
                return;
            throw new AssertionError("LHS had no content for " + rhs.getKey());
        }
        else if (rcf == null)
            throw new AssertionError("RHS had no content for " + lhs.getKey());
        assertEquals(lcf.deletionInfo(), rcf.deletionInfo());
        // iterate columns
        while (lhs.hasNext())
        {
            Cell clhs = (Cell)lhs.next();
            assert rhs.hasNext() : "LHS contained more columns than RHS for " + lhs.getKey();
            Cell crhs = (Cell)rhs.next();

            assertEquals("Mismatched columns for " + lhs.getKey(), clhs, crhs);
        }
        assert !rhs.hasNext() : "RHS contained more columns than LHS for " + lhs.getKey();
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

        public SSTableReader write(Set<String> keys) throws IOException
        {
            Map<String, ColumnFamily> map = new HashMap<String, ColumnFamily>();
            for (String key : keys)
            {
                ColumnFamily cf = ArrayBackedSortedColumns.factory.create(ksname, cfname);
                cf.addColumn(new BufferCell(Util.cellname(key), ByteBufferUtil.bytes(key), 0));
                map.put(key, cf);
            }
            return write(map);
        }

        public SSTableReader write(SortedMap<DecoratedKey, ColumnFamily> sorted) throws IOException
        {
            final Iterator<Map.Entry<DecoratedKey, ColumnFamily>> iter = sorted.entrySet().iterator();
            return write(sorted.size(), new Appender()
            {
                @Override
                public boolean append(SSTableWriter writer) throws IOException
                {
                    if (!iter.hasNext())
                        return false;
                    Map.Entry<DecoratedKey, ColumnFamily> entry = iter.next();
                    writer.append(entry.getKey(), entry.getValue());
                    return true;
                }
            });
        }

        public SSTableReader write(Map<String, ColumnFamily> entries) throws IOException
        {
            SortedMap<DecoratedKey, ColumnFamily> sorted = new TreeMap<DecoratedKey, ColumnFamily>();
            for (Map.Entry<String, ColumnFamily> entry : entries.entrySet())
                sorted.put(Util.dk(entry.getKey()), entry.getValue());

            return write(sorted);
        }

        public SSTableReader write(int expectedSize, Appender appender) throws IOException
        {
            File datafile = (dest == null) ? tempSSTableFile(ksname, cfname, generation) : new File(dest.filenameFor(Component.DATA));
            SSTableWriter writer = new SSTableWriter(datafile.getAbsolutePath(), expectedSize, ActiveRepairService.UNREPAIRED_SSTABLE);
            while (appender.append(writer)) { /* pass */ }
            SSTableReader reader = writer.closeAndOpenReader();
            // mark all components for removal
            if (cleanup)
                for (Component component : reader.components)
                    new File(reader.descriptor.filenameFor(component)).deleteOnExit();
            return reader;
        }
    }

    public static abstract class Appender
    {
        /** Called with an open writer until it returns false. */
        public abstract boolean append(SSTableWriter writer) throws IOException;
    }
}
