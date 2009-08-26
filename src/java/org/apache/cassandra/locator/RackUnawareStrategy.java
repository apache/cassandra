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
package org.apache.cassandra.locator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.net.EndPoint;

/**
 * This class returns the nodes responsible for a given
 * key but does not respect rack awareness. Basically
 * returns the 3 nodes that lie right next to each other
 * on the ring.
 */
public class RackUnawareStrategy extends AbstractStrategy
{
    public RackUnawareStrategy(TokenMetadata tokenMetadata, IPartitioner partitioner, int replicas, int storagePort)
    {
        super(tokenMetadata, partitioner, replicas, storagePort);
    }

    public EndPoint[] getStorageEndPoints(Token token)
    {
        return getStorageEndPoints(token, tokenMetadata_.cloneTokenEndPointMap());            
    }
    
    public EndPoint[] getStorageEndPointsForWrite(Token token)
    {
        Map<Token, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        Map<Token, EndPoint> bootstrapTokensToEndpointMap = tokenMetadata_.cloneBootstrapNodes();
        List<Token> tokenList = getStorageTokens(token, tokenToEndPointMap, bootstrapTokensToEndpointMap);
        List<EndPoint> list = new ArrayList<EndPoint>();
        for (Token t: tokenList)
        {
            EndPoint e = tokenToEndPointMap.get(t);
            if (e == null) 
                e = bootstrapTokensToEndpointMap.get(t); 
            assert e != null;
            list.add(e);
        }
        retrofitPorts(list);
        return list.toArray(new EndPoint[list.size()]);            
    }
    
    public EndPoint[] getStorageEndPoints(Token token, Map<Token, EndPoint> tokenToEndPointMap)
    {
        List<Token> tokenList = getStorageTokens(token, tokenToEndPointMap, null);
        List<EndPoint> list = new ArrayList<EndPoint>();
        for (Token t: tokenList)
            list.add(tokenToEndPointMap.get(t));
        retrofitPorts(list);
        return list.toArray(new EndPoint[list.size()]);
    }

    private List<Token> getStorageTokens(Token token, Map<Token, EndPoint> tokenToEndPointMap, Map<Token, EndPoint> bootStrapTokenToEndPointMap)
    {
        int startIndex;
        List<Token> tokenList = new ArrayList<Token>();
        int foundCount = 0;
        List tokens = new ArrayList<Token>(tokenToEndPointMap.keySet());
        List<Token> bsTokens = null;
        
        if (bootStrapTokenToEndPointMap != null)
        {
            bsTokens = new ArrayList<Token>(bootStrapTokenToEndPointMap.keySet());
            tokens.addAll(bsTokens);
        }
        Collections.sort(tokens);
        int index = Collections.binarySearch(tokens, token);
        if(index < 0)
        {
            index = (index + 1) * (-1);
            if (index >= tokens.size())
                index = 0;
        }
        int totalNodes = tokens.size();
        // Add the token at the index by default
        tokenList.add((Token)tokens.get(index));
        if (bsTokens == null || !bsTokens.contains(tokens.get(index)))
            foundCount++;
        startIndex = (index + 1)%totalNodes;
        // If we found N number of nodes we are good. This loop will just exit. Otherwise just
        // loop through the list and add until we have N nodes.
        for (int i = startIndex, count = 1; count < totalNodes && foundCount < replicas_; ++count, i = (i+1)%totalNodes)
        {
            if(!tokenList.contains(tokens.get(i)))
            {
                tokenList.add((Token)tokens.get(i));
                //Don't count bootstrapping tokens towards the count
                if (bsTokens==null || !bsTokens.contains(tokens.get(i)))
                    foundCount++;
            }
        }
        return tokenList;
    }
            
    public Map<String, EndPoint[]> getStorageEndPoints(String[] keys)
    {
    	Map<String, EndPoint[]> results = new HashMap<String, EndPoint[]>();

        for ( String key : keys )
        {
            results.put(key, getStorageEndPoints(partitioner_.getToken(key)));
        }
        return results;
    }
}
