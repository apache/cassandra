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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

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
        File tempdir = File.createTempFile(tablename, cfname);
        if(!tempdir.delete() || !tempdir.mkdir())
            throw new IOException("Temporary directory creation failed.");
        tempdir.deleteOnExit();
        File tabledir = new File(tempdir, tablename);
        tabledir.mkdir();
        tabledir.deleteOnExit();
        File datafile = new File(new Descriptor(tabledir, tablename, cfname, 0, false).filenameFor("Data.db"));
        if (!datafile.createNewFile())
            throw new IOException("unable to create file " + datafile);
        datafile.deleteOnExit();
        return datafile;
    }

    public static SSTableReader writeSSTable(Set<String> keys) throws IOException
    {
        Map<String, ColumnFamily> map = new HashMap<String, ColumnFamily>();
        for (String key : keys)
        {
            ColumnFamily cf = ColumnFamily.create(TABLENAME, CFNAME);
            cf.addColumn(new Column(ByteBuffer.wrap(key.getBytes()), ByteBuffer.wrap(key.getBytes()), 0));
            map.put(key, cf);
        }
        return writeSSTable(map);
    }

    public static SSTableReader writeSSTable(Map<String, ColumnFamily> entries) throws IOException
    {
        Map<ByteBuffer, ByteBuffer> map = new HashMap<ByteBuffer, ByteBuffer>();
        for (Map.Entry<String, ColumnFamily> entry : entries.entrySet())
        {
            DataOutputBuffer buffer = new DataOutputBuffer();
            ColumnFamily.serializer().serializeWithIndexes(entry.getValue(), buffer);
            map.put(ByteBuffer.wrap(entry.getKey().getBytes()), ByteBuffer.wrap(Arrays.copyOf(buffer.getData(), buffer.getLength())));
        }
        return writeRawSSTable(TABLENAME, CFNAME, map);
    }

    public static SSTableReader writeRawSSTable(String tablename, String cfname, Map<ByteBuffer, ByteBuffer> entries) throws IOException
    {
        File datafile = tempSSTableFile(tablename, cfname);
        SSTableWriter writer = new SSTableWriter(datafile.getAbsolutePath(), entries.size());
        SortedMap<DecoratedKey, ByteBuffer> sortedEntries = new TreeMap<DecoratedKey, ByteBuffer>();
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : entries.entrySet())
            sortedEntries.put(writer.partitioner.decorateKey(entry.getKey()), entry.getValue());
        for (Map.Entry<DecoratedKey, ByteBuffer> entry : sortedEntries.entrySet())
            writer.append(entry.getKey(), entry.getValue());
        new File(writer.descriptor.filenameFor(Component.PRIMARY_INDEX)).deleteOnExit();
        new File(writer.descriptor.filenameFor(Component.FILTER)).deleteOnExit();
        return writer.closeAndOpenReader();
    }

}
