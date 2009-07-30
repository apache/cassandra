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
package org.apache.cassandra.db;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import static org.junit.Assert.assertNull;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.filter.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.SSTableReader;

public class ColumnFamilyStoreTest extends CleanupHelper
{
    static byte[] bytes1, bytes2;

    static
    {
        Random random = new Random();
        bytes1 = new byte[1024];
        bytes2 = new byte[128];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
    }

    @Test
    public void testGetCompactionBuckets() throws IOException
    {
        // create files 20 40 60 ... 180
        List<String> small = new ArrayList<String>();
        List<String> med = new ArrayList<String>();
        List<String> all = new ArrayList<String>();

        String fname;
        fname = createFile(20);
        small.add(fname);
        all.add(fname);
        fname = createFile(40);
        small.add(fname);
        all.add(fname);

        for (int i = 60; i <= 140; i += 20)
        {
            fname = createFile(i);
            med.add(fname);
            all.add(fname);
        }

        Set<List<String>> buckets = ColumnFamilyStore.getCompactionBuckets(all, 50);
        assert buckets.size() == 2 : bucketString(buckets);
        Iterator<List<String>> iter = buckets.iterator();
        List<String> bucket1 = iter.next();
        List<String> bucket2 = iter.next();
        assert bucket1.size() + bucket2.size() == all.size() : bucketString(buckets) + " does not match [" + StringUtils.join(all, ", ") + "]";
        assert buckets.contains(small) : bucketString(buckets) + " does not contain {" + StringUtils.join(small, ", ") + "}";
        assert buckets.contains(med) : bucketString(buckets) + " does not contain {" + StringUtils.join(med, ", ") + "}";
    }

    private static String bucketString(Set<List<String>> buckets)
    {
        ArrayList<String> pieces = new ArrayList<String>();
        for (List<String> bucket : buckets)
        {
            pieces.add("[" + StringUtils.join(bucket, ", ") + "]");
        }
        return "{" + StringUtils.join(pieces, ", ") + "}";
    }

    private String createFile(int nBytes) throws IOException
    {
        File f = File.createTempFile("bucket_test", "");
        FileOutputStream fos = new FileOutputStream(f);
        byte[] bytes = new byte[nBytes];
        fos.write(bytes);
        fos.close();
        return f.getAbsolutePath();
    }

    @Test
    public void testGetColumnWithWrongBF() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Keyspace1", "key1");
        rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), "asdf".getBytes(), 0);
        rm.add(new QueryPath("Standard1", null, "Column2".getBytes()), "asdf".getBytes(), 0);
        rm.apply();
        store.forceBlockingFlush();

        List<SSTableReader> ssTables = table.getAllSSTablesOnDisk();
        assertEquals(1, ssTables.size());
        ssTables.get(0).forceBloomFilterFailures();
        ColumnFamily cf = store.getColumnFamily(new IdentityQueryFilter("key2", new QueryPath("Standard1", null, "Column1".getBytes())));
        assertNull(cf);
    }
}
