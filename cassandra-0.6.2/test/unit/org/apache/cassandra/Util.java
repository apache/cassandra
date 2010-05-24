package org.apache.cassandra;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.SliceRange;

public class Util
{
    public static Column column(String name, String value, long timestamp)
    {
        return new Column(name.getBytes(), value.getBytes(), timestamp);
    }

    public static void addMutation(RowMutation rm, String columnFamilyName, String superColumnName, long columnName, String value, long timestamp)
    {
        rm.add(new QueryPath(columnFamilyName, superColumnName.getBytes(), getBytes(columnName)), value.getBytes(), timestamp);
    }

    public static byte[] getBytes(long v)
    {
        byte[] bytes = new byte[8];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putLong(v);
        return bytes;
    }
    
    public static File createTemporarySSTable(String keyspace, String colFam) throws IOException
    {
        File tmpDir = new File(System.getProperty("java.io.tmpdir") + File.separator + keyspace);
        tmpDir.mkdirs();    // Create the per-keyspace temp directory
        return File.createTempFile(colFam + "-", "-Data.db", tmpDir);
    }

    public static RangeSliceReply getRangeSlice(ColumnFamilyStore cfs) throws IOException, ExecutionException, InterruptedException
    {
        Token min = StorageService.getPartitioner().getMinimumToken();
        return cfs.getRangeSlice(ArrayUtils.EMPTY_BYTE_ARRAY,
                                 new Bounds(min, min),
                                 10000,
                                 new SliceRange(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, 10000),
                                 null);
    }

    /**
     * Writes out a bunch of rows for a single column family.
     *
     * @param rows A group of RowMutations for the same table and column family.
     * @return The ColumnFamilyStore that was used.
     */
    public static ColumnFamilyStore writeColumnFamily(List<RowMutation> rms) throws IOException, ExecutionException, InterruptedException
    {
        RowMutation first = rms.get(0);
        String tablename = first.getTable();
        String cfname = first.columnFamilyNames().iterator().next();

        Table table = Table.open(tablename);
        ColumnFamilyStore store = table.getColumnFamilyStore(cfname);

        for (RowMutation rm : rms)
            rm.apply();

        store.forceBlockingFlush();
        return store;
    }
}
