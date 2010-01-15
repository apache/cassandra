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
package org.apache.cassandra.dht;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import com.google.common.collect.Multimap;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class BootStrapperTest
{
    @Test
    public void testGuessToken() throws IOException
    {
        StorageService ss = StorageService.instance;

        generateFakeEndpoints(3);

        InetAddress one = InetAddress.getByName("127.0.0.2");
        InetAddress two = InetAddress.getByName("127.0.0.3");
        InetAddress three = InetAddress.getByName("127.0.0.4");
        Map<InetAddress, Double> load = new HashMap<InetAddress, Double>();
        load.put(one, 1.0);
        load.put(two, 2.0);
        load.put(three, 3.0);

        TokenMetadata tmd = ss.getTokenMetadata();
        InetAddress source = BootStrapper.getBootstrapSource(tmd, load);
        assert three.equals(source);

        InetAddress myEndpoint = InetAddress.getByName("127.0.0.1");
        Range range3 = ss.getPrimaryRangeForEndPoint(three);
        Token fakeToken = ((IPartitioner)StorageService.getPartitioner()).midpoint(range3.left(), range3.right());
        assert range3.contains(fakeToken);
        ss.onChange(myEndpoint, StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + ss.getPartitioner().getTokenFactory().toString(fakeToken)));
        tmd = ss.getTokenMetadata();

        InetAddress source2 = BootStrapper.getBootstrapSource(tmd, load);
        assert two.equals(source2) : source2;
    }

    @Test
    public void testSourceTargetComputation() throws UnknownHostException
    {
        testSourceTargetComputation(1);
        testSourceTargetComputation(3);
        testSourceTargetComputation(100);
    }

    private void testSourceTargetComputation(int numOldNodes) throws UnknownHostException
    {
        StorageService ss = StorageService.instance;

        generateFakeEndpoints(numOldNodes);
        Token myToken = StorageService.getPartitioner().getRandomToken();
        InetAddress myEndpoint = InetAddress.getByName("127.0.0.1");

        TokenMetadata tmd = ss.getTokenMetadata();
        assertEquals(numOldNodes, tmd.sortedTokens().size());
        BootStrapper b = new BootStrapper(ss.getReplicationStrategy(), myEndpoint, myToken, tmd);
        Multimap<Range, InetAddress> res = b.getRangesWithSources();
        
        int transferCount = 0;
        for (Map.Entry<Range, Collection<InetAddress>> e : res.asMap().entrySet())
        {
            assert e.getValue() != null && e.getValue().size() > 0 : StringUtils.join(e.getValue(), ", ");
            transferCount++;
        }

        /* Only 1 transfer from old node to new node */
        assertEquals(1, transferCount);
        IFailureDetector mockFailureDetector = new IFailureDetector()
        {
            public boolean isAlive(InetAddress ep)
            {
                return true;
            }

            public void interpret(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void report(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
        };
        Multimap<InetAddress, Range> temp = BootStrapper.getWorkMap(res, mockFailureDetector);
        assertEquals(1, temp.keySet().size());
        assertEquals(1, temp.asMap().values().iterator().next().size());
        assert !temp.keySet().iterator().next().equals(myEndpoint);
    }

    private void generateFakeEndpoints(int numOldNodes) throws UnknownHostException
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner<?> p = StorageService.getPartitioner();

        for (int i = 1; i <= numOldNodes; i++)
        {
            // leave .1 for myEndpoint
            tmd.updateNormalToken(p.getRandomToken(), InetAddress.getByName("127.0.0." + (i + 1)));
        }
    }
}
