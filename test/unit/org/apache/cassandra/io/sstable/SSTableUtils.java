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

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class SSTableUtils
{
    // first configured table and cf
    public static String TABLENAME = "Keyspace1";
    public static String CFNAME = "Standard1";

    public static ColumnFamily createCF(long mfda, int ldt, IColumn... cols)
    {
        ColumnFamily cf = ColumnFamily.create(TABLENAME, CFNAME);
        cf.delete(ldt, mfda);
        for (IColumn col : cols)
            cf.addColumn(col);
        return cf;
    }

    public static File tempSSTableFile(String tablename, String cfname) throws IOException
    {
        return tempSSTableFile(tablename, cfname, 0);
    }

    public static File tempSSTableFile(String tablename, String cfname, int generation) throws IOException
    {
        File tempdir = File.createTempFile(tablename, cfname);
        if(!tempdir.delete() || !tempdir.mkdir())
            throw new IOException("Temporary directory creation failed.");
        tempdir.deleteOnExit();
        File tabledir = new File(tempdir, tablename);
        tabledir.mkdir();
        tabledir.deleteOnExit();
        File datafile = new File(new Descriptor(tabledir, tablename, cfname, generation, false).filenameFor("Data.db"));
        if (!datafile.createNewFile())
            throw new IOException("unable to create file " + datafile);
        datafile.deleteOnExit();
        return datafile;
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
        private String ksname = TABLENAME;
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
                ColumnFamily cf = ColumnFamily.create(ksname, cfname);
                cf.addColumn(new Column(ByteBuffer.wrap(key.getBytes()), ByteBuffer.wrap(key.getBytes()), 0));
                map.put(key, cf);
            }
            return write(map);
        }

        public SSTableReader write(Map<String, ColumnFamily> entries) throws IOException
        {
            Map<ByteBuffer, ByteBuffer> map = new HashMap<ByteBuffer, ByteBuffer>();
            for (Map.Entry<String, ColumnFamily> entry : entries.entrySet())
            {
                DataOutputBuffer buffer = new DataOutputBuffer();
                ColumnFamily.serializer().serializeWithIndexes(entry.getValue(), buffer);
                map.put(ByteBuffer.wrap(entry.getKey().getBytes()),
                        ByteBuffer.wrap(buffer.asByteArray()));
            }
            return writeRaw(map);
        }

        /**
         * @Deprecated: Writes the binary content of a row, which should be encapsulated.
         */
        public SSTableReader writeRaw(Map<ByteBuffer, ByteBuffer> entries) throws IOException
        {
            File datafile = (dest == null) ? tempSSTableFile(ksname, cfname, generation) : new File(dest.filenameFor(Component.DATA));
            SSTableWriter writer = new SSTableWriter(datafile.getAbsolutePath(), entries.size());
            SortedMap<DecoratedKey, ByteBuffer> sortedEntries = new TreeMap<DecoratedKey, ByteBuffer>();
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : entries.entrySet())
                sortedEntries.put(writer.partitioner.decorateKey(entry.getKey()), entry.getValue());
            for (Map.Entry<DecoratedKey, ByteBuffer> entry : sortedEntries.entrySet())
                writer.append(entry.getKey(), entry.getValue());
            SSTableReader reader = writer.closeAndOpenReader();
            if (cleanup)
                for (Component comp : reader.components)
                    new File(reader.descriptor.filenameFor(comp)).deleteOnExit();
            return reader;
        }
    }
}
