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

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.CompactionIterator.CompactedRow;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import static org.apache.cassandra.service.AntiEntropyService.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptorTest;
import org.apache.cassandra.Util;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class AntiEntropyServiceTest extends CleanupHelper
{
    // table and column family to test against
    public static AntiEntropyService aes;

    public static String tablename;
    public static String cfname;
    public static InetAddress LOCAL, REMOTE;

    @BeforeClass
    public static void prepareClass() throws Exception
    {
        LOCAL = FBUtilities.getLocalAddress();
        tablename = "Keyspace4";
        StorageService.instance.initServer();
        // generate a fake endpoint for which we can spoof receiving/sending trees
        REMOTE = InetAddress.getByName("127.0.0.2");
        cfname = Table.open(tablename).getColumnFamilies().iterator().next();
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
    }

    @Test
    public void testInstance() throws Throwable
    {
        assert null != aes;
        assert aes == AntiEntropyService.instance;
    }

    @Test
    public void testGetValidator() throws Throwable
    {
        // not major
        assert aes.getValidator(tablename, cfname, null, false) instanceof NoopValidator;
        // triggered manually
        assert aes.getValidator(tablename, cfname, REMOTE, true) instanceof Validator;
    }

    @Test
    public void testValidatorPrepare() throws Throwable
    {
        Validator validator;

        // write
        List<RowMutation> rms = new LinkedList<RowMutation>();
        RowMutation rm;
        rm = new RowMutation(tablename, "key1");
        rm.add(new QueryPath(cfname, null, "Column1".getBytes()), "asdf".getBytes(), 0);
        rms.add(rm);
        Util.writeColumnFamily(rms);

        // sample
        validator = new Validator(new CFPair(tablename, cfname));
        validator.prepare();

        // and confirm that the tree was split
        assertTrue(validator.tree.size() > 1);
    }
    
    @Test
    public void testValidatorComplete() throws Throwable
    {
        Validator validator = new Validator(new CFPair(tablename, cfname));
        validator.prepare();
        validator.complete();

        // confirm that the tree was validated
        Token min = validator.tree.partitioner().getMinimumToken();
        assert null != validator.tree.hash(new Range(min, min));

        // wait for queued operations to be flushed
        flushAES().get(5000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testValidatorAdd() throws Throwable
    {
        Validator validator = new Validator(new CFPair(tablename, cfname));
        IPartitioner part = validator.tree.partitioner();
        Token min = part.getMinimumToken();
        Token mid = part.midpoint(min, min);
        validator.prepare();

        // add a row with the minimum token
        validator.add(new CompactedRow(new DecoratedKey(min, "nonsense!"),
                                       new DataOutputBuffer()));

        // and a row after it
        validator.add(new CompactedRow(new DecoratedKey(mid, "inconceivable!"),
                                       new DataOutputBuffer()));
        validator.complete();

        // confirm that the tree was validated
        assert null != validator.tree.hash(new Range(min, min));
    }

    /**
     * Build a column family with 2 or more SSTables, and then force a major compaction
     */
    @Test
    public void testTreeStore() throws Throwable
    {
        // populate column family
        List<RowMutation> rms = new LinkedList<RowMutation>();
        RowMutation rm = new RowMutation(tablename, "key");
        rm.add(new QueryPath(cfname, null, "Column1".getBytes()), "asdf".getBytes(), 0);
        rms.add(rm);
        // with two SSTables
        Util.writeColumnFamily(rms);
        ColumnFamilyStore store = Util.writeColumnFamily(rms);
        
        TreePair old = aes.getRendezvousPair_TestsOnly(tablename, cfname, REMOTE);
        // force a readonly compaction, and wait for it to finish
        CompactionManager.instance.submitReadonly(store, REMOTE).get(5000, TimeUnit.MILLISECONDS);

        // check that a tree was created and stored
        flushAES().get(5000, TimeUnit.MILLISECONDS);
        assert old != aes.getRendezvousPair_TestsOnly(tablename, cfname, REMOTE);
    }

    @Test
    public void testNotifyNeighbors() throws Throwable
    {
        // generate empty tree
        Validator validator = new Validator(new CFPair(tablename, cfname));
        validator.prepare();
        validator.complete();

        // grab reference to the tree
        MerkleTree tree = validator.tree;

        // notify ourself (should immediately be delivered into AE_STAGE)
        aes.notifyNeighbors(validator, LOCAL, Arrays.asList(LOCAL));
        flushAES().get(5, TimeUnit.SECONDS);
        
        // confirm that our reference is not equal to the original due
        // to (de)serialization
        assert tree != aes.getRendezvousPair_TestsOnly(tablename, cfname, REMOTE).left;
    }

    @Test
    public void testGetNeighborsPlusOne() throws Throwable
    {
        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + 1 + DatabaseDescriptor.getReplicationFactor(tablename));
        expected.remove(FBUtilities.getLocalAddress());
        assertEquals(expected, AntiEntropyService.getNeighbors(tablename));
    }

    @Test
    public void testGetNeighborsTimesTwo() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(1 + (2 * DatabaseDescriptor.getReplicationFactor(tablename)));
        AbstractReplicationStrategy ars = StorageService.instance.getReplicationStrategy(tablename);
        Set<InetAddress> expected = new HashSet<InetAddress>();
        for (Range replicaRange : ars.getAddressRanges(tablename).get(FBUtilities.getLocalAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd, tablename).get(replicaRange));
        }
        expected.remove(FBUtilities.getLocalAddress());
        assertEquals(expected, AntiEntropyService.getNeighbors(tablename));
    }

    @Test
    public void testDifferencer() throws Throwable
    {
        // generate a tree
        Validator validator = new Validator(new CFPair("Keyspace1", "lcf"));
        validator.prepare();

        // create a clone with no values filled
        validator.complete();
        MerkleTree ltree = validator.tree;
        validator = new Validator(new CFPair("Keyspace1", "rcf"));
        validator.prepare();
        validator.complete();
        MerkleTree rtree = validator.tree;

        // change a range in one of the trees
        Token min = StorageService.instance.getPartitioner().getMinimumToken();
        ltree.invalidate(min);
        MerkleTree.TreeRange changed = ltree.invalids(new Range(min, min)).next();
        changed.hash("non-empty hash!".getBytes());

        // difference the trees
        Differencer diff = new Differencer(new CFPair(tablename, cfname),
                                           LOCAL, LOCAL, ltree, rtree);
        diff.run();
        
        // ensure that the changed range was recorded
        assertEquals("Wrong number of differing ranges", 1, diff.differences.size());
        assertEquals("Wrong differing range", changed, diff.differences.get(0));
    }

    Set<InetAddress> addTokens(int max) throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        Set<InetAddress> endpoints = new HashSet<InetAddress>();
        for (int i = 1; i < max; i++)
        {
            InetAddress endpoint = InetAddress.getByName("127.0.0." + i);
            tmd.updateNormalToken(StorageService.getPartitioner().getRandomToken(), endpoint);
            endpoints.add(endpoint);
        }
        return endpoints;
    }

    Future<Object> flushAES()
    {
        return StageManager.getStage(StageManager.AE_SERVICE_STAGE).submit(new Callable<Object>()
        {
            public Boolean call()
            {
                return true;
            }
        });
    }
}
