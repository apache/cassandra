/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.epaxos;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;

public class EpaxosRangeExecutionInfoTest extends AbstractEpaxosTest
{
    private static String DC2 = "DC2";

    private static InetAddress LOCAL_ADDRESS;
    private static InetAddress REMOTE_ADDRESS;

    static
    {
        try
        {
            LOCAL_ADDRESS = InetAddress.getByName("127.0.0.2");
            REMOTE_ADDRESS = InetAddress.getByName("127.0.0.3");
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    private static boolean ksManagerContains(EpaxosService service, Scope scope, QueryInstance instance)
    {
        return service.getKeyStateManager(scope).loadKeyState(instance.getQuery().getCfKey()).contains(instance.getId());
    }

    private MockMultiDcService service = null;
    Iterator<Pair<ByteBuffer, Map<Scope, ExecutionInfo>>> iter;
    Pair<ByteBuffer, Map<Scope, ExecutionInfo>> next;

    private static final ReplayPosition PAST = new ReplayPosition(0l, 0);
    private static final ReplayPosition PRESENT = new ReplayPosition(1l, 0);
    private static final ReplayPosition FUTURE = new ReplayPosition(2l, 0);

    @Before
    public void setUp() throws Exception
    {
        clearAll();

        service = new MockMultiDcService();
        service.dcs.put(LOCALHOST, DC1);
        service.dcs.put(LOCAL_ADDRESS, DC1);
        service.dcs.put(REMOTE_ADDRESS, DC2);

        iter = null;
        next = null;
    }

    private QueryInstance createQueryInstance(int key, ConsistencyLevel cl, ReplayPosition position) throws InvalidInstanceStateChange
    {
        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(key, key, cl));
        instance.preaccept(service.getCurrentDependencies(instance).left);
        service.recordExecuted(instance, position, 0l);
        return instance;
    }

    /**
     * Tests that the single iterator case isn't totally broken
     */
    @Test
    public void singleScope() throws Exception
    {
        // create global
        createQueryInstance(1, ConsistencyLevel.SERIAL, PAST);
        createQueryInstance(1, ConsistencyLevel.SERIAL, PRESENT);
        createQueryInstance(2, ConsistencyLevel.SERIAL, PRESENT);
        createQueryInstance(3, ConsistencyLevel.SERIAL, PRESENT);
        createQueryInstance(3, ConsistencyLevel.SERIAL, FUTURE);

        Map<Scope, ExecutionInfo> expected = new HashMap<>();
        iter = service.getRangeExecutionInfo(cfm.cfId, new Range<>(token(0), token(10)), PRESENT, LOCAL_ADDRESS);

        // 1st key
        Assert.assertTrue(iter.hasNext());
        next = iter.next();
        expected.clear(); expected.put(Scope.GLOBAL, new ExecutionInfo(0l, 2l));
        Assert.assertEquals(key(1), next.left);
        Assert.assertEquals(expected, next.right);

        // 2nd key
        Assert.assertTrue(iter.hasNext());
        next = iter.next();
        expected.clear(); expected.put(Scope.GLOBAL, new ExecutionInfo(0l, 1l));
        Assert.assertEquals(key(2), next.left);
        Assert.assertEquals(expected, next.right);

        // 3rd key
        Assert.assertTrue(iter.hasNext());
        next = iter.next();
        expected.clear(); expected.put(Scope.GLOBAL, new ExecutionInfo(0l, 1l));
        Assert.assertEquals(key(3), next.left);
        Assert.assertEquals(expected, next.right);
    }

    @Test
    public void dcLocal2Scopes() throws Exception
    {
        // create global
        createQueryInstance(1, ConsistencyLevel.SERIAL, PAST);
        createQueryInstance(1, ConsistencyLevel.SERIAL, FUTURE);
        createQueryInstance(2, ConsistencyLevel.SERIAL, PRESENT);

        // create local
        createQueryInstance(2, ConsistencyLevel.LOCAL_SERIAL, PAST);
        createQueryInstance(2, ConsistencyLevel.LOCAL_SERIAL, FUTURE);
        createQueryInstance(3, ConsistencyLevel.LOCAL_SERIAL, PAST);
        createQueryInstance(3, ConsistencyLevel.LOCAL_SERIAL, PRESENT);

        Map<Scope, ExecutionInfo> expected = new HashMap<>();
        iter = service.getRangeExecutionInfo(cfm.cfId, new Range<>(token(0), token(10)), PRESENT, LOCAL_ADDRESS);

        // 1st key, should only have global
        Assert.assertTrue(iter.hasNext());
        next = iter.next();
        expected.clear(); expected.put(Scope.GLOBAL, new ExecutionInfo(0l, 1l));
        Assert.assertEquals(key(1), next.left);
        Assert.assertEquals(Hex.bytesToHex(next.left.array()), expected, next.right);

        // 2nd key, should only have both
        Assert.assertTrue(iter.hasNext());
        next = iter.next();
        expected.clear();
        expected.put(Scope.GLOBAL, new ExecutionInfo(0l, 1l));
        expected.put(Scope.LOCAL, new ExecutionInfo(0l, 1l));
        Assert.assertEquals(key(2), next.left);
        Assert.assertEquals(Hex.bytesToHex(next.left.array()), expected, next.right);

        // 3rd key, should only have local
        Assert.assertTrue(iter.hasNext());
        next = iter.next();
        expected.clear(); expected.put(Scope.LOCAL, new ExecutionInfo(0l, 2l));
        Assert.assertEquals(key(3), next.left);
        Assert.assertEquals(Hex.bytesToHex(next.left.array()), expected, next.right);

        Assert.assertFalse(iter.hasNext());
    }


    @Test
    public void dcRemote2Scopes() throws Exception
    {
        // create global
        createQueryInstance(1, ConsistencyLevel.SERIAL, PAST);
        createQueryInstance(1, ConsistencyLevel.SERIAL, FUTURE);
        createQueryInstance(2, ConsistencyLevel.SERIAL, PRESENT);

        // create local
        createQueryInstance(2, ConsistencyLevel.LOCAL_SERIAL, PAST);
        createQueryInstance(2, ConsistencyLevel.LOCAL_SERIAL, FUTURE);
        createQueryInstance(3, ConsistencyLevel.LOCAL_SERIAL, PAST);
        createQueryInstance(3, ConsistencyLevel.LOCAL_SERIAL, PRESENT);

        Map<Scope, ExecutionInfo> expected = new HashMap<>();
        iter = service.getRangeExecutionInfo(cfm.cfId, new Range<>(token(0), token(10)), PRESENT, REMOTE_ADDRESS);

        // 1st key, should only have global
        Assert.assertTrue(iter.hasNext());
        next = iter.next();
        expected.clear(); expected.put(Scope.GLOBAL, new ExecutionInfo(0l, 1l));
        Assert.assertEquals(key(1), next.left);
        Assert.assertEquals(Hex.bytesToHex(next.left.array()), expected, next.right);

        // 2nd key, should only have both
        Assert.assertTrue(iter.hasNext());
        next = iter.next();
        expected.clear();
        expected.put(Scope.GLOBAL, new ExecutionInfo(0l, 1l));
        Assert.assertEquals(key(2), next.left);
        Assert.assertEquals(Hex.bytesToHex(next.left.array()), expected, next.right);

        Assert.assertFalse(iter.hasNext());
    }
}
