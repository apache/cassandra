package org.apache.cassandra.locator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;


/**
 * This class returns the nodes responsible for a given
 * key but does not respect rack awareness. Basically
 * returns the 3 nodes that lie right next to each other
 * on the ring.
 */
public class RackUnawareStrategy extends AbstractStrategy
{   
    /* Use this flag to check if initialization is in order. */
    private AtomicBoolean initialized_ = new AtomicBoolean(false);
    private Map<Range, List<EndPoint>> rangeToEndPointMap_;
    
    public RackUnawareStrategy(TokenMetadata tokenMetadata)
    {
        super(tokenMetadata);
    }
    
    public EndPoint[] getStorageEndPoints(BigInteger token)
    {
        return getStorageEndPoints(token, tokenMetadata_.cloneTokenEndPointMap());            
    }
    
    public EndPoint[] getStorageEndPoints(BigInteger token, Map<BigInteger, EndPoint> tokenToEndPointMap)
    {
        int startIndex;
        List<EndPoint> list = new ArrayList<EndPoint>();
        int foundCount = 0;
        int N = DatabaseDescriptor.getReplicationFactor();
        List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap.keySet());
        Collections.sort(tokens);
        int index = Collections.binarySearch(tokens, token);
        if(index < 0)
        {
            index = (index + 1) * (-1);
            if (index >= tokens.size())
                index = 0;
        }
        int totalNodes = tokens.size();
        // Add the node at the index by default
        list.add(tokenToEndPointMap.get(tokens.get(index)));
        foundCount++;
        startIndex = (index + 1)%totalNodes;
        // If we found N number of nodes we are good. This loop will just exit. Otherwise just
        // loop through the list and add until we have N nodes.
        for (int i = startIndex, count = 1; count < totalNodes && foundCount < N; ++count, i = (i+1)%totalNodes)
        {
            if( ! list.contains(tokenToEndPointMap.get(tokens.get(i))))
            {
                list.add(tokenToEndPointMap.get(tokens.get(i)));
                foundCount++;
            }
        }
        retrofitPorts(list);
        return list.toArray(new EndPoint[list.size()]);
    }
            
    public Map<String, EndPoint[]> getStorageEndPoints(String[] keys)
    {
    	Map<String, EndPoint[]> results = new HashMap<String, EndPoint[]>();

        for ( String key : keys )
        {
        	BigInteger token = StorageService.hash(key);
            results.put(key, getStorageEndPoints(token));
        }

        return results;
    }
}
