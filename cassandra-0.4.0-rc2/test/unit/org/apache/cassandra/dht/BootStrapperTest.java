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

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;
import org.junit.Test;

public class BootStrapperTest {
    @Test
    public void testSourceTargetComputation() 
    {
        int numOldNodes = 3;
        IPartitioner p = generateOldTokens(numOldNodes);
        
        Token newToken = p.getDefaultToken();
        EndPoint newEndPoint = new EndPoint("1.2.3.10",100);
 
        /* New token needs to be part of the map for the algorithm
         * to calculate the ranges correctly
         */
        StorageService.instance().updateTokenMetadata(newToken, newEndPoint);

        BootStrapper b = new BootStrapper(new EndPoint[]{newEndPoint}, newToken );
        Map<Range,List<BootstrapSourceTarget>> res = b.getRangesWithSourceTarget();
        
        int transferCount = 0;
        for ( Map.Entry<Range, List<BootstrapSourceTarget>> e: res.entrySet())
        {
            if (e.getValue() != null && e.getValue().size() >0)
            {
                transferCount++;
            }
        }
        /* Only 1 transfer from old node to new node */
        assertEquals(1, transferCount);
        Map<EndPoint, Map<EndPoint,List<Range>>> temp = LeaveJoinProtocolHelper.getWorkMap(res);
        assertEquals(1, temp.keySet().size());
        assertEquals(1, temp.entrySet().size());

        Map<EndPoint,Map<EndPoint,List<Range>>> res2 = LeaveJoinProtocolHelper.filterRangesForTargetEndPoint(temp, newEndPoint);
        /* After filtering, still only 1 transfer */
        assertEquals(1, res2.keySet().size());
        assertEquals(1, res2.entrySet().size());
        assertTrue(((Map<EndPoint,List<Range>>)res2.values().toArray()[0]).containsKey(newEndPoint));
    }

    private IPartitioner generateOldTokens(int numOldNodes)
    {
        IPartitioner p = new RandomPartitioner();
        for (int i = 0 ; i< numOldNodes; i++)
        {
            EndPoint e  = new EndPoint("127.0.0."+i, 100);
            Token t = p.getDefaultToken();
            StorageService.instance().updateTokenMetadata(t, e);
        }
        return p;
    }
}