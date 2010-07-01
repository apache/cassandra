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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import static com.google.common.base.Charsets.UTF_8;

public class Util
{
    public static DecoratedKey dk(String key)
    {
        return StorageService.getPartitioner().decorateKey(key.getBytes(UTF_8));
    }

    public static Column column(String name, String value, IClock clock)
    {
        return new Column(name.getBytes(), value.getBytes(), clock);
    }

    public static Range range(IPartitioner p, String left, String right)
    {
        return new Range(p.getToken(left.getBytes()), p.getToken(right.getBytes()));
    }

    public static void addMutation(RowMutation rm, String columnFamilyName, String superColumnName, long columnName, String value, IClock clock)
    {
        rm.add(new QueryPath(columnFamilyName, superColumnName.getBytes(), getBytes(columnName)), value.getBytes(), clock);
    }

    public static byte[] getBytes(long v)
    {
        byte[] bytes = new byte[8];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putLong(v);
        return bytes;
    }
    
    public static List<Row> getRangeSlice(ColumnFamilyStore cfs) throws IOException, ExecutionException, InterruptedException
    {
        Token min = StorageService.getPartitioner().getMinimumToken();
        return cfs.getRangeSlice(null,
                                 new Bounds(min, min),
                                 10000,
                                 new IdentityQueryFilter());
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
        String cfname = first.getColumnFamilies().iterator().next().metadata().cfName;

        Table table = Table.open(tablename);
        ColumnFamilyStore store = table.getColumnFamilyStore(cfname);

        for (RowMutation rm : rms)
            rm.apply();

        store.forceBlockingFlush();
        return store;
    }

    public static ColumnFamily getColumnFamily(Table table, DecoratedKey key, String cfName) throws IOException
    {
        ColumnFamilyStore cfStore = table.getColumnFamilyStore(cfName);
        assert cfStore != null : "Column family " + cfName + " has not been defined";
        return cfStore.getColumnFamily(QueryFilter.getIdentityFilter(key, new QueryPath(cfName)));
    }
}
