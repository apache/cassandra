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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

import org.apache.cassandra.config.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.PrecompactedRow;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

import static org.apache.cassandra.service.AntiEntropyService.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class AntiEntropyServiceTestAbstract extends CleanupHelper
{
    // table and column family to test against
    public AntiEntropyService aes;

    public String tablename;
    public String cfname;
    public TreeRequest request;
    public ColumnFamilyStore store;
    public InetAddress LOCAL, REMOTE;

    public Range local_range;

    private boolean initialized;

    public abstract void init();

    public abstract List<IMutation> getWriteData();

    @Before
    public void prepare() throws Exception
    {
        if (!initialized)
        {
            initialized = true;

            init();

            LOCAL = FBUtilities.getBroadcastAddress();
            StorageService.instance.initServer(0);
            // generate a fake endpoint for which we can spoof receiving/sending trees
            REMOTE = InetAddress.getByName("127.0.0.2");
            store = null;
            for (ColumnFamilyStore cfs : Table.open(tablename).getColumnFamilyStores())
            {
                if (cfs.columnFamily.equals(cfname))
                {
                    store = cfs;
                    break;
                }
            }
            assert store != null : "CF not found: " + cfname;
        }

        aes = AntiEntropyService.instance;
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        StorageService.instance.setToken(StorageService.getPartitioner().getRandomToken());
        tmd.updateNormalToken(StorageService.getPartitioner().getMinimumToken(), REMOTE);
        assert tmd.isMember(REMOTE);

        Gossiper.instance.initializeNodeUnsafe(REMOTE, 1);

        local_range = StorageService.instance.getLocalPrimaryRange();

        // (we use REMOTE instead of LOCAL so that the reponses for the validator.complete() get lost)
        request = new TreeRequest(UUID.randomUUID().toString(), REMOTE, local_range, new CFPair(tablename, cfname));
        // Set a fake session corresponding to this fake request
        AntiEntropyService.instance.submitArtificialRepairSession(request, tablename, cfname);
    }

    @After
    public void teardown() throws Exception
    {
        flushAES();
    }

    @Test
    public void testValidatorPrepare() throws Throwable
    {
        Validator validator;

        // write
        Util.writeColumnFamily(getWriteData());

        // sample
        validator = new Validator(request);
        validator.prepare(store);

        // and confirm that the tree was split
        assertTrue(validator.tree.size() > 1);
    }
    
    @Test
    public void testValidatorComplete() throws Throwable
    {
        Validator validator = new Validator(request);
        validator.prepare(store);
        validator.completeTree();

        // confirm that the tree was validated
        Token min = validator.tree.partitioner().getMinimumToken();
        assert null != validator.tree.hash(new Range(min, min));
    }

    @Test
    public void testValidatorAdd() throws Throwable
    {
        Validator validator = new Validator(request);
        IPartitioner part = validator.tree.partitioner();
        Token mid = part.midpoint(local_range.left, local_range.right);
        validator.prepare(store);

        // add a row
        validator.add(new PrecompactedRow(new DecoratedKey(mid, ByteBufferUtil.bytes("inconceivable!")),
                                          ColumnFamily.create(Schema.instance.getCFMetaData(tablename, cfname))));
        validator.completeTree();

        // confirm that the tree was validated
        assert null != validator.tree.hash(local_range);
    }

    @Test
    public void testGetNeighborsPlusOne() throws Throwable
    {
        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + Table.open(tablename).getReplicationStrategy().getReplicationFactor());
        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<Range> ranges = StorageService.instance.getLocalRanges(tablename);
        Set<InetAddress> neighbors = new HashSet<InetAddress>();
        for (Range range : ranges)
        {
            neighbors.addAll(AntiEntropyService.getNeighbors(tablename, range));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwo() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * Table.open(tablename).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Table.open(tablename).getReplicationStrategy();
        Set<InetAddress> expected = new HashSet<InetAddress>();
        for (Range replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd).get(replicaRange));
        }
        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<Range> ranges = StorageService.instance.getLocalRanges(tablename);
        Set<InetAddress> neighbors = new HashSet<InetAddress>();
        for (Range range : ranges)
        {
            neighbors.addAll(AntiEntropyService.getNeighbors(tablename, range));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testDifferencer() throws Throwable
    {
        // this next part does some housekeeping so that cleanup in the differencer doesn't error out.
        AntiEntropyService.RepairFuture sess = AntiEntropyService.instance.submitArtificialRepairSession(request, tablename, cfname);

        // generate a tree
        Validator validator = new Validator(request);
        validator.prepare(store);
        validator.completeTree();
        MerkleTree ltree = validator.tree;

        // and a clone
        validator = new Validator(request);
        validator.prepare(store);
        validator.completeTree();
        MerkleTree rtree = validator.tree;

        // change a range in one of the trees
        Token ltoken = StorageService.getPartitioner().midpoint(local_range.left, local_range.right);
        ltree.invalidate(ltoken);
        MerkleTree.TreeRange changed = ltree.get(ltoken);
        changed.hash("non-empty hash!".getBytes());

        Set<Range> interesting = new HashSet<Range>();
        interesting.add(changed);

        // difference the trees
        // note: we reuse the same endpoint which is bogus in theory but fine here
        AntiEntropyService.TreeResponse r1 = new AntiEntropyService.TreeResponse(REMOTE, ltree);
        AntiEntropyService.TreeResponse r2 = new AntiEntropyService.TreeResponse(REMOTE, rtree);
        AntiEntropyService.RepairSession.Differencer diff = sess.session.new Differencer(cfname, r1, r2);
        diff.run();
        
        // ensure that the changed range was recorded
        assertEquals("Wrong differing ranges", interesting, new HashSet<Range>(diff.differences));
    }

    Set<InetAddress> addTokens(int max) throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        Set<InetAddress> endpoints = new HashSet<InetAddress>();
        for (int i = 1; i <= max; i++)
        {
            InetAddress endpoint = InetAddress.getByName("127.0.0." + i);
            tmd.updateNormalToken(StorageService.getPartitioner().getRandomToken(), endpoint);
            endpoints.add(endpoint);
        }
        return endpoints;
    }

    void flushAES() throws Exception
    {
        final ThreadPoolExecutor stage = StageManager.getStage(Stage.ANTI_ENTROPY);
        final Callable noop = new Callable<Object>()
        {
            public Boolean call()
            {
                return true;
            }
        };
        
        // send two tasks through the stage: one to follow existing tasks and a second to follow tasks created by
        // those existing tasks: tasks won't recursively create more tasks
        stage.submit(noop).get(5000, TimeUnit.MILLISECONDS);
        stage.submit(noop).get(5000, TimeUnit.MILLISECONDS);
    }
}
