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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import com.google.common.collect.Multimap;

public class BootStrapperTest {
    @Test
    public void testSourceTargetComputation() throws UnknownHostException
    {
        testSourceTargetComputation(1);
        testSourceTargetComputation(3);
        testSourceTargetComputation(100);
    }

    private void testSourceTargetComputation(int numOldNodes) throws UnknownHostException
    {
        StorageService ss = StorageService.instance();

        generateFakeEndpoints(numOldNodes);
        
        Token myToken = StorageService.getPartitioner().getDefaultToken();
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
        TokenMetadata tmd = StorageService.instance().getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner<?> p = StorageService.getPartitioner();

        for (int i = 1; i <= numOldNodes; i++)
        {
            // leave .1 for myEndpoint
            tmd.update(p.getDefaultToken(), InetAddress.getByName("127.0.0." + (i + 1)));
        }
    }
}