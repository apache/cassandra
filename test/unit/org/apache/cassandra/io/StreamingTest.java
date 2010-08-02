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
package org.apache.cassandra.io;

import static junit.framework.Assert.assertEquals;

import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamOut;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.Test;

public class StreamingTest extends CleanupHelper
{
    public static final InetAddress LOCAL = FBUtilities.getLocalAddress();

    @Test
    public void testTransferTable() throws Exception
    {
        StorageService.instance.initServer();

        // write a temporary SSTable, but don't register it
        Set<String> content = new HashSet<String>();
        content.add("key");
        content.add("key2");
        content.add("key3");
        SSTableReader sstable = SSTableUtils.writeSSTable(content);
        String tablename = sstable.getTableName();
        String cfname = sstable.getColumnFamilyName();

        // transfer the first and last key
        IPartitioner p = StorageService.getPartitioner();
        List<Range> ranges = new ArrayList<Range>();
        ranges.add(new Range(p.getMinimumToken(), p.getToken("key".getBytes())));
        ranges.add(new Range(p.getToken("key2".getBytes()), p.getMinimumToken()));
        StreamOut.transferSSTables(LOCAL, tablename, Arrays.asList(sstable), ranges);

        // confirm that the SSTable was transferred and registered
        ColumnFamilyStore cfstore = Table.open(tablename).getColumnFamilyStore(cfname);
        List<Row> rows = Util.getRangeSlice(cfstore);
        assertEquals(2, rows.size());
        assert Arrays.equals(rows.get(0).key.key, "key".getBytes());
        assert Arrays.equals(rows.get(1).key.key, "key3".getBytes());
        assert rows.get(0).cf.getColumnsMap().size() == 1;
        assert rows.get(1).cf.getColumnsMap().size() == 1;
        assert rows.get(1).cf.getColumn("key3".getBytes()) != null;

        // and that the index and filter were properly recovered
        assert null != cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("key"), new QueryPath("Standard1")));
        assert null != cfstore.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("key3"), new QueryPath("Standard1")));
    }
}
