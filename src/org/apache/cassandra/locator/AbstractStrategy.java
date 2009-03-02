package org.apache.cassandra.locator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;
import org.apache.log4j.Logger;

/**
 * This class contains a helper method that will be used by
 * all abstraction that implement the IReplicaPlacementStrategy
 * interface.
*/
public abstract class AbstractStrategy implements IReplicaPlacementStrategy
{
    protected static Logger logger_ = Logger.getLogger(AbstractStrategy.class);
    
    protected TokenMetadata tokenMetadata_;
    
    AbstractStrategy(TokenMetadata tokenMetadata)
    {
        tokenMetadata_ = tokenMetadata;
    }
    
    /*
     * This method changes the ports of the endpoints from
     * the control port to the storage ports.
    */
    protected void retrofitPorts(List<EndPoint> eps)
    {
        for ( EndPoint ep : eps )
        {
            ep.setPort(DatabaseDescriptor.getStoragePort());
        }
    }

    protected EndPoint getNextAvailableEndPoint(EndPoint startPoint, List<EndPoint> topN, List<EndPoint> liveNodes)
    {
        EndPoint endPoint = null;
        Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap.keySet());
        Collections.sort(tokens);
        BigInteger token = tokenMetadata_.getToken(startPoint);
        int index = Collections.binarySearch(tokens, token);
        if(index < 0)
        {
            index = (index + 1) * (-1);
            if (index >= tokens.size())
                index = 0;
        }
        int totalNodes = tokens.size();
        int startIndex = (index+1)%totalNodes;
        for (int i = startIndex, count = 1; count < totalNodes ; ++count, i = (i+1)%totalNodes)
        {
            EndPoint tmpEndPoint = tokenToEndPointMap.get(tokens.get(i));
            if(FailureDetector.instance().isAlive(tmpEndPoint) && !topN.contains(tmpEndPoint) && !liveNodes.contains(tmpEndPoint))
            {
                endPoint = tmpEndPoint;
                break;
            }
        }
        return endPoint;
    }

    /*
     * This method returns the hint map. The key is the endpoint
     * on which the data is being placed and the value is the
     * endpoint which is in the top N.
     * Get the map of top N to the live nodes currently.
     */
    public Map<EndPoint, EndPoint> getHintedStorageEndPoints(BigInteger token)
    {
        List<EndPoint> liveList = new ArrayList<EndPoint>();
        Map<EndPoint, EndPoint> map = new HashMap<EndPoint, EndPoint>();
        EndPoint[] topN = getStorageEndPoints( token );

        for( int i = 0 ; i < topN.length ; i++)
        {
            if( FailureDetector.instance().isAlive(topN[i]))
            {
                map.put(topN[i], topN[i]);
                liveList.add(topN[i]) ;
            }
            else
            {
                EndPoint endPoint = getNextAvailableEndPoint(topN[i], Arrays.asList(topN), liveList);
                if(endPoint != null)
                {
                    map.put(endPoint, topN[i]);
                    liveList.add(endPoint) ;
                }
                else
                {
                    // log a warning , maybe throw an exception
                    logger_.warn("Unable to find a live Endpoint we might be out of live nodes , This is dangerous !!!!");
                }
            }
        }
        return map;
    }

    public abstract EndPoint[] getStorageEndPoints(BigInteger token);

}
