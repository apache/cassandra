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

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;

import static org.junit.Assert.assertTrue;

public class Util
{
    private static List<UUID> hostIdPool = new ArrayList<UUID>();

    public static DecoratedKey dk(String key)
    {
        return StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(key));
    }

    public static DecoratedKey dk(String key, AbstractType type)
    {
        return StorageService.getPartitioner().decorateKey(type.fromString(key));
    }

    public static DecoratedKey dk(ByteBuffer key)
    {
        return StorageService.getPartitioner().decorateKey(key);
    }

    public static RowPosition rp(String key)
    {
        return rp(key, StorageService.getPartitioner());
    }

    public static RowPosition rp(String key, IPartitioner partitioner)
    {
        return RowPosition.ForKey.get(ByteBufferUtil.bytes(key), partitioner);
    }

    public static CellName cellname(ByteBuffer... bbs)
    {
        if (bbs.length == 1)
            return CellNames.simpleDense(bbs[0]);
        else
            return CellNames.compositeDense(bbs);
    }

    public static CellName cellname(String... strs)
    {
        ByteBuffer[] bbs = new ByteBuffer[strs.length];
        for (int i = 0; i < strs.length; i++)
            bbs[i] = ByteBufferUtil.bytes(strs[i]);
        return cellname(bbs);
    }

    public static CellName cellname(int i)
    {
        return CellNames.simpleDense(ByteBufferUtil.bytes(i));
    }

    public static CellName cellname(long l)
    {
        return CellNames.simpleDense(ByteBufferUtil.bytes(l));
    }

    public static Cell column(String name, String value, long timestamp)
    {
        return new BufferCell(cellname(name), ByteBufferUtil.bytes(value), timestamp);
    }

    public static Cell expiringColumn(String name, String value, long timestamp, int ttl)
    {
        return new BufferExpiringCell(cellname(name), ByteBufferUtil.bytes(value), timestamp, ttl);
    }

    public static Token token(String key)
    {
        return StorageService.getPartitioner().getToken(ByteBufferUtil.bytes(key));
    }

    public static Range<RowPosition> range(String left, String right)
    {
        return new Range<RowPosition>(rp(left), rp(right));
    }

    public static Range<RowPosition> range(IPartitioner p, String left, String right)
    {
        return new Range<RowPosition>(rp(left, p), rp(right, p));
    }

    public static Bounds<RowPosition> bounds(String left, String right)
    {
        return new Bounds<RowPosition>(rp(left), rp(right));
    }

    public static void addMutation(Mutation rm, String columnFamilyName, String superColumnName, long columnName, String value, long timestamp)
    {
        CellName cname = superColumnName == null
                       ? CellNames.simpleDense(getBytes(columnName))
                       : CellNames.compositeDense(ByteBufferUtil.bytes(superColumnName), getBytes(columnName));
        rm.add(columnFamilyName, cname, ByteBufferUtil.bytes(value), timestamp);
    }

    public static ByteBuffer getBytes(long v)
    {
        byte[] bytes = new byte[8];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putLong(v);
        bb.rewind();
        return bb;
    }

    public static ByteBuffer getBytes(int v)
    {
        byte[] bytes = new byte[4];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putInt(v);
        bb.rewind();
        return bb;
    }

    public static List<Row> getRangeSlice(ColumnFamilyStore cfs)
    {
        return getRangeSlice(cfs, null);
    }

    public static List<Row> getRangeSlice(ColumnFamilyStore cfs, ByteBuffer superColumn)
    {
        IDiskAtomFilter filter = superColumn == null
                               ? new IdentityQueryFilter()
                               : new SliceQueryFilter(SuperColumns.startOf(superColumn), SuperColumns.endOf(superColumn), false, Integer.MAX_VALUE);

        Token min = StorageService.getPartitioner().getMinimumToken();
        return cfs.getRangeSlice(Bounds.makeRowBounds(min, min), null, filter, 10000);
    }

    /**
     * Writes out a bunch of mutations for a single column family.
     *
     * @param mutations A group of Mutations for the same keyspace and column family.
     * @return The ColumnFamilyStore that was used.
     */
    public static ColumnFamilyStore writeColumnFamily(List<Mutation> mutations)
    {
        IMutation first = mutations.get(0);
        String keyspaceName = first.getKeyspaceName();
        UUID cfid = first.getColumnFamilyIds().iterator().next();

        for (Mutation rm : mutations)
            rm.applyUnsafe();

        ColumnFamilyStore store = Keyspace.open(keyspaceName).getColumnFamilyStore(cfid);
        store.forceBlockingFlush();
        return store;
    }

    public static ColumnFamily getColumnFamily(Keyspace keyspace, DecoratedKey key, String cfName)
    {
        ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore(cfName);
        assert cfStore != null : "Table " + cfName + " has not been defined";
        return cfStore.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName, System.currentTimeMillis()));
    }

    public static boolean equalsCounterId(CounterId n, ByteBuffer context, int offset)
    {
        return CounterId.wrap(context, context.position() + offset).equals(n);
    }

    public static ColumnFamily cloneAndRemoveDeleted(ColumnFamily cf, int gcBefore)
    {
        return ColumnFamilyStore.removeDeleted(cf.cloneMe(), gcBefore);
    }

    /**
     * Creates initial set of nodes and tokens. Nodes are added to StorageService as 'normal'
     */
    public static void createInitialRing(StorageService ss, IPartitioner partitioner, List<Token> endpointTokens,
                                   List<Token> keyTokens, List<InetAddress> hosts, List<UUID> hostIds, int howMany)
        throws UnknownHostException
    {
        // Expand pool of host IDs as necessary
        for (int i = hostIdPool.size(); i < howMany; i++)
            hostIdPool.add(UUID.randomUUID());

        for (int i=0; i<howMany; i++)
        {
            endpointTokens.add(new BigIntegerToken(String.valueOf(10 * i)));
            keyTokens.add(new BigIntegerToken(String.valueOf(10 * i + 5)));
            hostIds.add(hostIdPool.get(i));
        }

        for (int i=0; i<endpointTokens.size(); i++)
        {
            InetAddress ep = InetAddress.getByName("127.0.0." + String.valueOf(i + 1));
            Gossiper.instance.initializeNodeUnsafe(ep, hostIds.get(i), 1);
            Gossiper.instance.injectApplicationState(ep, ApplicationState.TOKENS, new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(endpointTokens.get(i))));
            ss.onChange(ep,
                        ApplicationState.STATUS,
                        new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(endpointTokens.get(i))));
            hosts.add(ep);
        }

        // check that all nodes are in token metadata
        for (int i=0; i<endpointTokens.size(); ++i)
            assertTrue(ss.getTokenMetadata().isMember(hosts.get(i)));
    }

    public static Future<?> compactAll(ColumnFamilyStore cfs, int gcBefore)
    {
        List<Descriptor> descriptors = new ArrayList<>();
        for (SSTableReader sstable : cfs.getSSTables())
            descriptors.add(sstable.descriptor);
        return CompactionManager.instance.submitUserDefined(cfs, descriptors, gcBefore);
    }

    public static void compact(ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
    {
        int gcBefore = cfs.gcBefore(System.currentTimeMillis());
        AbstractCompactionTask task = cfs.getCompactionStrategy().getUserDefinedTask(sstables, gcBefore);
        task.execute(null);
    }

    public static void expectEOF(Callable<?> callable)
    {
        expectException(callable, EOFException.class);
    }

    public static void expectException(Callable<?> callable, Class<?> exception)
    {
        boolean thrown = false;

        try
        {
            callable.call();
        }
        catch (Throwable e)
        {
            assert e.getClass().equals(exception) : e.getClass().getName() + " is not " + exception.getName();
            thrown = true;
        }

        assert thrown : exception.getName() + " not received";
    }

    public static QueryFilter namesQueryFilter(ColumnFamilyStore cfs, DecoratedKey key)
    {
        SortedSet<CellName> s = new TreeSet<CellName>(cfs.getComparator());
        return QueryFilter.getNamesFilter(key, cfs.name, s, System.currentTimeMillis());
    }

    public static QueryFilter namesQueryFilter(ColumnFamilyStore cfs, DecoratedKey key, String... names)
    {
        SortedSet<CellName> s = new TreeSet<CellName>(cfs.getComparator());
        for (String str : names)
            s.add(cellname(str));
        return QueryFilter.getNamesFilter(key, cfs.name, s, System.currentTimeMillis());
    }

    public static QueryFilter namesQueryFilter(ColumnFamilyStore cfs, DecoratedKey key, CellName... names)
    {
        SortedSet<CellName> s = new TreeSet<CellName>(cfs.getComparator());
        for (CellName n : names)
            s.add(n);
        return QueryFilter.getNamesFilter(key, cfs.name, s, System.currentTimeMillis());
    }

    public static NamesQueryFilter namesFilter(ColumnFamilyStore cfs, String... names)
    {
        SortedSet<CellName> s = new TreeSet<CellName>(cfs.getComparator());
        for (String str : names)
            s.add(cellname(str));
        return new NamesQueryFilter(s);
    }

    public static String string(ByteBuffer bb)
    {
        try
        {
            return ByteBufferUtil.string(bb);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static RangeTombstone tombstone(String start, String finish, long timestamp, int localtime)
    {
        Composite startName = CellNames.simpleDense(ByteBufferUtil.bytes(start));
        Composite endName = CellNames.simpleDense(ByteBufferUtil.bytes(finish));
        return new RangeTombstone(startName, endName, timestamp , localtime);
    }
}
