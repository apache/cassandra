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

import java.util.*;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.RackUnawareStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.gms.ApplicationState;

public class MoveTest
{
    /**
     * Test whether write endpoints is correct when the node is leaving. Uses
     * StorageService.onChange and does not manipulate token metadata directly.
     */
    @Test
    public void testWriteEndPointsDuringLeave() throws UnknownHostException
    {
        StorageService ss = StorageService.instance();

        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, 3);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        AbstractReplicationStrategy oldStrategy = ss.setReplicationStrategyUnsafe(testStrategy);

        ArrayList<Token> endPointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        createInitialRing(ss, partitioner, endPointTokens, keyTokens, hosts, 5);

        // Third node leaves
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(endPointTokens.get(2))));

        // check that it is correctly marked as leaving in tmd
        assertTrue(tmd.isLeaving(hosts.get(2)));

        // check that pending ranges are correct (primary range should go to 1st node, first
        // replica range to 4th node and 2nd replica range to 5th node)
        assertTrue(tmd.getPendingRanges(hosts.get(0)).get(0).equals(new Range(endPointTokens.get(1),
                                                                              endPointTokens.get(2))));
        assertTrue(tmd.getPendingRanges(hosts.get(3)).get(0).equals(new Range(endPointTokens.get(4),
                                                                              endPointTokens.get(0))));
        assertTrue(tmd.getPendingRanges(hosts.get(4)).get(0).equals(new Range(endPointTokens.get(0),
                                                                              endPointTokens.get(1))));

        for (int i=0; i<keyTokens.size(); ++i)
        {
            Collection<InetAddress> endPoints = testStrategy.getWriteEndpoints(keyTokens.get(i), testStrategy.getNaturalEndpoints(keyTokens.get(i)));

            // Original third node does not store replicas for 4th and 5th node (ranges 20-30
            // and 30-40 respectively), so their write endpoints count should be still 3. The
            // third node stores data for ranges 40-0, 0-10 and 10-20, so writes falling to
            // these ranges should have four endpoints now. keyTokens[2] is 25 and keyTokens[3]
            // is 35, so these are the ones that should have 3 endpoints.
            if (i==2 || i==3)
                assertTrue(endPoints.size() == 3);
            else
                assertTrue(endPoints.size() == 4);
        }

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategy);
    }

    /**
     * Test pending ranges and write endpoints when multiple nodes are on the move
     * simultaneously
     */
    @Test
    public void testSimultaneousMove() throws UnknownHostException
    {
        StorageService ss = StorageService.instance();
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, 3);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        AbstractReplicationStrategy oldStrategy = ss.setReplicationStrategyUnsafe(testStrategy);

        ArrayList<Token> endPointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 10 nodes
        createInitialRing(ss, partitioner, endPointTokens, keyTokens, hosts, 10);

        // nodes 6, 8 and 9 leave
        ss.onChange(hosts.get(6), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(endPointTokens.get(6))));
        ss.onChange(hosts.get(8), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(endPointTokens.get(8))));
        ss.onChange(hosts.get(9), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(endPointTokens.get(9))));
        
        // boot two new nodes with keyTokens.get(5) and keyTokens.get(7)
        InetAddress boot1 = InetAddress.getByName("127.0.1.1");
        ss.onChange(boot1, StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(5))));
        InetAddress boot2 = InetAddress.getByName("127.0.1.2");
        ss.onChange(boot2, StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(7))));

        Collection<InetAddress> endPoints = null;

        // tokens 5, 15 and 25 should go three nodes
        for (int i=0; i<3; ++i)
        {
            endPoints = testStrategy.getWriteEndpoints(keyTokens.get(i), testStrategy.getNaturalEndpoints(keyTokens.get(i)));
            assertTrue(endPoints.size() == 3);
            assertTrue(endPoints.contains(hosts.get(i+1)));
            assertTrue(endPoints.contains(hosts.get(i+2)));
            assertTrue(endPoints.contains(hosts.get(i+3)));
        }

        // token 35 should go to nodes 4, 5, 6, 7 and boot1
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(3), testStrategy.getNaturalEndpoints(keyTokens.get(3)));
        assertTrue(endPoints.size() == 5);
        assertTrue(endPoints.contains(hosts.get(4)));
        assertTrue(endPoints.contains(hosts.get(5)));
        assertTrue(endPoints.contains(hosts.get(6)));
        assertTrue(endPoints.contains(hosts.get(7)));
        assertTrue(endPoints.contains(boot1));

        // token 45 should go to nodes 5, 6, 7, 0, boot1 and boot2
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(4), testStrategy.getNaturalEndpoints(keyTokens.get(4)));
        assertTrue(endPoints.size() == 6);
        assertTrue(endPoints.contains(hosts.get(5)));
        assertTrue(endPoints.contains(hosts.get(6)));
        assertTrue(endPoints.contains(hosts.get(7)));
        assertTrue(endPoints.contains(hosts.get(0)));
        assertTrue(endPoints.contains(boot1));
        assertTrue(endPoints.contains(boot2));

        // token 55 should go to nodes 6, 7, 8, 0, 1, boot1 and boot2
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(5), testStrategy.getNaturalEndpoints(keyTokens.get(5)));
        assertTrue(endPoints.size() == 7);
        assertTrue(endPoints.contains(hosts.get(6)));
        assertTrue(endPoints.contains(hosts.get(7)));
        assertTrue(endPoints.contains(hosts.get(8)));
        assertTrue(endPoints.contains(hosts.get(0)));
        assertTrue(endPoints.contains(hosts.get(1)));
        assertTrue(endPoints.contains(boot1));
        assertTrue(endPoints.contains(boot2));

        // token 65 should go to nodes 7, 8, 9, 0, 1 and boot2
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(6), testStrategy.getNaturalEndpoints(keyTokens.get(6)));
        assertTrue(endPoints.size() == 6);
        assertTrue(endPoints.contains(hosts.get(7)));
        assertTrue(endPoints.contains(hosts.get(8)));
        assertTrue(endPoints.contains(hosts.get(9)));
        assertTrue(endPoints.contains(hosts.get(0)));
        assertTrue(endPoints.contains(hosts.get(1)));
        assertTrue(endPoints.contains(boot2));

        // token 75 should to go nodes 8, 9, 0, 1, 2 and boot2
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(7), testStrategy.getNaturalEndpoints(keyTokens.get(7)));
        assertTrue(endPoints.size() == 6);
        assertTrue(endPoints.contains(hosts.get(8)));
        assertTrue(endPoints.contains(hosts.get(9)));
        assertTrue(endPoints.contains(hosts.get(0)));
        assertTrue(endPoints.contains(hosts.get(1)));
        assertTrue(endPoints.contains(hosts.get(2)));
        assertTrue(endPoints.contains(boot2));

        // token 85 should go to nodes 9, 0, 1 and 2
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(8), testStrategy.getNaturalEndpoints(keyTokens.get(8)));
        assertTrue(endPoints.size() == 4);
        assertTrue(endPoints.contains(hosts.get(9)));
        assertTrue(endPoints.contains(hosts.get(0)));
        assertTrue(endPoints.contains(hosts.get(1)));
        assertTrue(endPoints.contains(hosts.get(2)));

        // token 95 should go to nodes 0, 1 and 2
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(9), testStrategy.getNaturalEndpoints(keyTokens.get(9)));
        assertTrue(endPoints.size() == 3);
        assertTrue(endPoints.contains(hosts.get(0)));
        assertTrue(endPoints.contains(hosts.get(1)));
        assertTrue(endPoints.contains(hosts.get(2)));

        // Now finish node 6 and node 9 leaving, as well as boot1 (after this node 8 is still
        // leaving and boot2 in progress
        ss.onChange(hosts.get(6), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(endPointTokens.get(6))));
        ss.onChange(hosts.get(9), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(endPointTokens.get(9))));
        ss.onChange(boot1, StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(5))));

        // tokens 5, 15 and 25 should go three nodes
        for (int i=0; i<3; ++i)
        {
            endPoints = testStrategy.getWriteEndpoints(keyTokens.get(i), testStrategy.getNaturalEndpoints(keyTokens.get(i)));
            assertTrue(endPoints.size() == 3);
            assertTrue(endPoints.contains(hosts.get(i+1)));
            assertTrue(endPoints.contains(hosts.get(i+2)));
            assertTrue(endPoints.contains(hosts.get(i+3)));
        }

        // token 35 goes to nodes 4, 5 and boot1
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(3), testStrategy.getNaturalEndpoints(keyTokens.get(3)));
        assertTrue(endPoints.size() == 3);
        assertTrue(endPoints.contains(hosts.get(4)));
        assertTrue(endPoints.contains(hosts.get(5)));
        assertTrue(endPoints.contains(boot1));

        // token 45 goes to nodes 5, boot1 and node7
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(4), testStrategy.getNaturalEndpoints(keyTokens.get(4)));
        assertTrue(endPoints.size() == 3);
        assertTrue(endPoints.contains(hosts.get(5)));
        assertTrue(endPoints.contains(boot1));
        assertTrue(endPoints.contains(hosts.get(7)));

        // token 55 goes to boot1, 7, boot2, 8 and 0
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(5), testStrategy.getNaturalEndpoints(keyTokens.get(5)));
        assertTrue(endPoints.size() == 5);
        assertTrue(endPoints.contains(boot1));
        assertTrue(endPoints.contains(hosts.get(7)));
        assertTrue(endPoints.contains(boot2));
        assertTrue(endPoints.contains(hosts.get(8)));
        assertTrue(endPoints.contains(hosts.get(0)));

        // token 65 goes to nodes 7, boot2, 8, 0 and 1
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(6), testStrategy.getNaturalEndpoints(keyTokens.get(6)));
        assertTrue(endPoints.size() == 5);
        assertTrue(endPoints.contains(hosts.get(7)));
        assertTrue(endPoints.contains(boot2));
        assertTrue(endPoints.contains(hosts.get(8)));
        assertTrue(endPoints.contains(hosts.get(0)));
        assertTrue(endPoints.contains(hosts.get(1)));

        // token 75 goes to nodes boot2, 8, 0, 1 and 2
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(7), testStrategy.getNaturalEndpoints(keyTokens.get(7)));
        assertTrue(endPoints.size() == 5);
        assertTrue(endPoints.contains(boot2));
        assertTrue(endPoints.contains(hosts.get(8)));
        assertTrue(endPoints.contains(hosts.get(0)));
        assertTrue(endPoints.contains(hosts.get(1)));
        assertTrue(endPoints.contains(hosts.get(2)));

        // token 85 goes to nodes 0, 1 and 2
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(8), testStrategy.getNaturalEndpoints(keyTokens.get(8)));
        assertTrue(endPoints.size() == 3);
        assertTrue(endPoints.contains(hosts.get(0)));
        assertTrue(endPoints.contains(hosts.get(1)));
        assertTrue(endPoints.contains(hosts.get(2)));

        // token 95 goes to nodes 0, 1 and 2
        endPoints = testStrategy.getWriteEndpoints(keyTokens.get(9), testStrategy.getNaturalEndpoints(keyTokens.get(9)));
        assertTrue(endPoints.size() == 3);
        assertTrue(endPoints.contains(hosts.get(0)));
        assertTrue(endPoints.contains(hosts.get(1)));
        assertTrue(endPoints.contains(hosts.get(2)));

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategy);
    }

    @Test
    public void testStateJumpToBootstrap() throws UnknownHostException
    {
        StorageService ss = StorageService.instance();
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, 3);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        AbstractReplicationStrategy oldStrategy = ss.setReplicationStrategyUnsafe(testStrategy);

        ArrayList<Token> endPointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 5 nodes
        createInitialRing(ss, partitioner, endPointTokens, keyTokens, hosts, 5);

        // node 2 leaves
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(endPointTokens.get(2))));

        // don't bother to test pending ranges here, that is extensively tested by other
        // tests. Just check that the node is in appropriate lists.
        assertTrue(tmd.isMember(hosts.get(2)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().isEmpty());

        // Bootstrap the node immedidiately to keyTokens.get(4) without going through STATE_LEFT
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(4))));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(4)).equals(hosts.get(2)));

        // Bootstrap node hosts.get(3) to keyTokens.get(1)
        ss.onChange(hosts.get(3), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertFalse(tmd.isMember(hosts.get(3)));
        assertFalse(tmd.isLeaving(hosts.get(3)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(4)).equals(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(3)));

        // Bootstrap node hosts.get(2) further to keyTokens.get(3)
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(3))));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(3)).equals(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(4)) == null);
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(3)));

        // Go to normal again for both nodes
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(3))));
        ss.onChange(hosts.get(3), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(2))));

        assertTrue(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(3)));
        assertTrue(tmd.isMember(hosts.get(3)));
        assertFalse(tmd.isLeaving(hosts.get(3)));
        assertTrue(tmd.getToken(hosts.get(3)).equals(keyTokens.get(2)));

        assertTrue(tmd.getBootstrapTokens().isEmpty());

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategy);
    }

    @Test
    public void testStateJumpToNormal() throws UnknownHostException
    {
        StorageService ss = StorageService.instance();
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, 3);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        AbstractReplicationStrategy oldStrategy = ss.setReplicationStrategyUnsafe(testStrategy);

        ArrayList<Token> endPointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 5 nodes
        createInitialRing(ss, partitioner, endPointTokens, keyTokens, hosts, 5);

        // node 2 leaves
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(endPointTokens.get(2))));

        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getToken(hosts.get(2)).equals(endPointTokens.get(2)));

        // back to normal
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(2))));

        assertTrue(tmd.getLeavingEndPoints().isEmpty());
        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(2)));

        // node 3 goes through leave and left and then jumps to normal
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(2))));
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(2))));
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(4))));

        assertTrue(tmd.getBootstrapTokens().isEmpty());
        assertTrue(tmd.getLeavingEndPoints().isEmpty());
        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(4)));

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategy);
    }

    @Test
    public void testStateJumpToLeaving() throws UnknownHostException
    {
        StorageService ss = StorageService.instance();
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, 3);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        AbstractReplicationStrategy oldStrategy = ss.setReplicationStrategyUnsafe(testStrategy);

        ArrayList<Token> endPointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 5 nodes
        createInitialRing(ss, partitioner, endPointTokens, keyTokens, hosts, 5);

        // node 2 leaves with _different_ token
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(0))));

        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(0)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getEndPoint(endPointTokens.get(2)) == null);

        // go to boostrap
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().size() == 1);
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(2)));

        // jump to leaving again
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertTrue(tmd.getEndPoint(keyTokens.get(1)).equals(hosts.get(2)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().isEmpty());

        // go to state left
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategy);
    }

    @Test
    public void testStateJumpToLeft() throws UnknownHostException
    {
        StorageService ss = StorageService.instance();
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, 3);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        AbstractReplicationStrategy oldStrategy = ss.setReplicationStrategyUnsafe(testStrategy);

        ArrayList<Token> endPointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 5 nodes
        createInitialRing(ss, partitioner, endPointTokens, keyTokens, hosts, 5);

        // node hosts.get(2) goes jumps to left
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(endPointTokens.get(2))));

        assertFalse(tmd.isMember(hosts.get(2)));

        // node hosts.get(4) goes to bootstrap
        ss.onChange(hosts.get(3), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertFalse(tmd.isMember(hosts.get(3)));
        assertTrue(tmd.getBootstrapTokens().size() == 1);
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(3)));

        // and then directly to 'left'
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertTrue(tmd.getBootstrapTokens().size() == 0);
        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategy);
    }

    /**
     * Creates initial set of nodes and tokens. Nodes are added to StorageService as 'normal'
     */
    private void createInitialRing(StorageService ss, IPartitioner partitioner, List<Token> endPointTokens,
                                   List<Token> keyTokens, List<InetAddress> hosts, int howMany)
        throws UnknownHostException
    {
        for (int i=0; i<howMany; i++)
        {
            endPointTokens.add(new BigIntegerToken(String.valueOf(10 * i)));
            keyTokens.add(new BigIntegerToken(String.valueOf(10 * i + 5)));
        }

        for (int i=0; i<endPointTokens.size(); i++)
        {
            InetAddress ep = InetAddress.getByName("127.0.0." + String.valueOf(i + 1));
            ss.onChange(ep, StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(endPointTokens.get(i))));
            hosts.add(ep);
        }

        // check that all nodes are in token metadata
        for (int i=0; i<endPointTokens.size(); ++i)
            assertTrue(ss.getTokenMetadata().isMember(hosts.get(i)));
    }

}
