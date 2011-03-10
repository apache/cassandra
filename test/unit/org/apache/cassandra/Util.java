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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.NodeId;

import static com.google.common.base.Charsets.UTF_8;

public class Util
{
    public static DecoratedKey dk(String key)
    {
        return StorageService.getPartitioner().decorateKey(ByteBuffer.wrap(key.getBytes(UTF_8)));
    }

    public static Column column(String name, String value, long timestamp)
    {
        return new Column(ByteBuffer.wrap(name.getBytes()), ByteBuffer.wrap(value.getBytes()), timestamp);
    }

    public static Token token(String key)
    {
        return StorageService.getPartitioner().getToken(ByteBuffer.wrap(key.getBytes()));
    }

    public static Range range(String left, String right)
    {
        return new Range(token(left), token(right));
    }

    public static Range range(IPartitioner p, String left, String right)
    {
        return new Range(p.getToken(ByteBuffer.wrap(left.getBytes())), p.getToken(ByteBuffer.wrap(right.getBytes())));
    }

    public static Bounds bounds(String left, String right)
    {
        return new Bounds(token(left), token(right));
    }

    public static void addMutation(RowMutation rm, String columnFamilyName, String superColumnName, long columnName, String value, long timestamp)
    {
        rm.add(new QueryPath(columnFamilyName, ByteBuffer.wrap(superColumnName.getBytes()), getBytes(columnName)), ByteBuffer.wrap(value.getBytes()), timestamp);
    }

    public static ByteBuffer getBytes(long v)
    {
        byte[] bytes = new byte[8];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putLong(v);
        bb.rewind();
        return bb;
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

    public static byte[] concatByteArrays(byte[] first, byte[]... remaining)
    {
        int length = first.length;
        for (byte[] array : remaining)
        {
            length += array.length;
        }

        byte[] result = new byte[length];
        System.arraycopy(first, 0, result, 0, first.length);
        int offset = first.length;

        for (byte[] array : remaining)
        {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }

        return result;
    }

    public static boolean equalsNodeId(NodeId n, ByteBuffer context, int offset)
    {
        return NodeId.wrap(context, context.position() + offset).equals(n);
    }

    public static ColumnFamily cloneAndRemoveDeleted(ColumnFamily cf, int gcBefore)
    {
        return ColumnFamilyStore.removeDeleted(cf.cloneMe(), gcBefore);
    }

    /**
     * Creates initial set of nodes and tokens. Nodes are added to StorageService as 'normal'
     */
    public static void createInitialRing(StorageService ss, IPartitioner partitioner, List<Token> endpointTokens,
                                   List<Token> keyTokens, List<InetAddress> hosts, int howMany)
        throws UnknownHostException
    {
        for (int i=0; i<howMany; i++)
        {
            endpointTokens.add(new BigIntegerToken(String.valueOf(10 * i)));
            keyTokens.add(new BigIntegerToken(String.valueOf(10 * i + 5)));
        }

        for (int i=0; i<endpointTokens.size(); i++)
        {
            InetAddress ep = InetAddress.getByName("127.0.0." + String.valueOf(i + 1));
            ss.onChange(ep, ApplicationState.STATUS, new VersionedValue.VersionedValueFactory(partitioner).normal(endpointTokens.get(i)));
            hosts.add(ep);
        }

        // check that all nodes are in token metadata
        for (int i=0; i<endpointTokens.size(); ++i)
            assertTrue(ss.getTokenMetadata().isMember(hosts.get(i)));
    }
}
