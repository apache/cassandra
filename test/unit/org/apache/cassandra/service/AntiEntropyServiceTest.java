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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.PrecompactedRow;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.cassandra.service.AntiEntropyService.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AntiEntropyServiceTest extends CleanupHelper
{
    // table and column family to test against
    public static AntiEntropyService aes;

    public static String tablename;
    public static String cfname;
    public static TreeRequest request;
    public static ColumnFamilyStore store;
    public static InetAddress LOCAL, REMOTE;

    @BeforeClass
    public static void prepareClass() throws Exception
    {
        LOCAL = FBUtilities.getLocalAddress();
        tablename = "Keyspace5";
        StorageService.instance.initServer();
        // generate a fake endpoint for which we can spoof receiving/sending trees
        REMOTE = InetAddress.getByName("127.0.0.2");
        store = Table.open(tablename).getColumnFamilyStores().iterator().next();
        cfname = store.columnFamily;
    }

    @Before
    public void prepare() throws Exception
    {
        aes = AntiEntropyService.instance;
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        tmd.updateNormalToken(StorageService.getPartitioner().getRandomToken(), LOCAL);
        tmd.updateNormalToken(StorageService.getPartitioner().getMinimumToken(), REMOTE);
        assert tmd.isMember(REMOTE);
        
        // random session id for each test
        request = new TreeRequest(UUID.randomUUID().toString(), LOCAL, new CFPair(tablename, cfname));
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
        List<RowMutation> rms = new LinkedList<RowMutation>();
        RowMutation rm;
        rm = new RowMutation(tablename, ByteBuffer.wrap("key1".getBytes()));
        rm.add(new QueryPath(cfname, null, ByteBuffer.wrap("Column1".getBytes())), ByteBuffer.wrap("asdf".getBytes()), 0);
        rms.add(rm);
        Util.writeColumnFamily(rms);

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
        validator.complete();

        // confirm that the tree was validated
        Token min = validator.tree.partitioner().getMinimumToken();
        assert null != validator.tree.hash(new Range(min, min));
    }

    @Test
    public void testValidatorAdd() throws Throwable
    {
        Validator validator = new Validator(request);
        IPartitioner part = validator.tree.partitioner();
        Token min = part.getMinimumToken();
        Token mid = part.midpoint(min, min);
        validator.prepare(store);

        // add a row with the minimum token
        validator.add(new PrecompactedRow(new DecoratedKey(min, ByteBuffer.wrap("nonsense!".getBytes(UTF_8))),
                                       new DataOutputBuffer()));

        // and a row after it
        validator.add(new PrecompactedRow(new DecoratedKey(mid, ByteBuffer.wrap("inconceivable!".getBytes(UTF_8))),
                                       new DataOutputBuffer()));
        validator.complete();

        // confirm that the tree was validated
        assert null != validator.tree.hash(new Range(min, min));
    }

    @Test
    public void testManualRepair() throws Throwable
    {
        AntiEntropyService.RepairSession sess = AntiEntropyService.instance.getRepairSession(tablename, cfname);
        sess.start();
        sess.blockUntilRunning();

        // ensure that the session doesn't end without a response from REMOTE
        sess.join(100);
        assert sess.isAlive();

        // deliver a fake response from REMOTE
        AntiEntropyService.instance.completedRequest(new TreeRequest(sess.getName(), REMOTE, request.cf));

        // block until the repair has completed
        sess.join();
    }

    @Test
    public void testGetNeighborsPlusOne() throws Throwable
    {
        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + DatabaseDescriptor.getReplicationFactor(tablename));
        expected.remove(FBUtilities.getLocalAddress());
        assertEquals(expected, AntiEntropyService.getNeighbors(tablename));
    }

    @Test
    public void testGetNeighborsTimesTwo() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * DatabaseDescriptor.getReplicationFactor(tablename));
        AbstractReplicationStrategy ars = Table.open(tablename).replicationStrategy;
        Set<InetAddress> expected = new HashSet<InetAddress>();
        for (Range replicaRange : ars.getAddressRanges().get(FBUtilities.getLocalAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd).get(replicaRange));
        }
        expected.remove(FBUtilities.getLocalAddress());
        assertEquals(expected, AntiEntropyService.getNeighbors(tablename));
    }

    @Test
    public void testDifferencer() throws Throwable
    {
        // generate a tree
        Validator validator = new Validator(request);
        validator.prepare(store);
        validator.complete();
        MerkleTree ltree = validator.tree;

        // and a clone
        validator = new Validator(request);
        validator.prepare(store);
        validator.complete();
        MerkleTree rtree = validator.tree;

        // change a range in one of the trees
        Token min = StorageService.instance.getPartitioner().getMinimumToken();
        ltree.invalidate(min);
        MerkleTree.TreeRange changed = ltree.invalids(new Range(min, min)).next();
        changed.hash("non-empty hash!".getBytes());

        // difference the trees
        Differencer diff = new Differencer(request, ltree, rtree);
        diff.run();
        
        // ensure that the changed range was recorded
        assertEquals("Wrong number of differing ranges", 1, diff.differences.size());
        assertEquals("Wrong differing range", changed, diff.differences.get(0));
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
        final ThreadPoolExecutor stage = StageManager.getStage(Stage.ANTIENTROPY);
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
