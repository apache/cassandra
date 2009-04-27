 /**
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

package org.apache.cassandra.dht;

 import java.util.ArrayList;
 import java.util.Collections;
 import java.util.HashMap;
 import java.util.HashSet;
 import java.util.List;
 import java.util.Map;
 import java.util.Set;

 import org.apache.log4j.Logger;

 import org.apache.cassandra.locator.TokenMetadata;
 import org.apache.cassandra.net.EndPoint;
 import org.apache.cassandra.service.StorageService;
 import org.apache.cassandra.utils.LogUtil;


/**
 * This class performs the exact opposite of the
 * operations of the Bootstrapper class. Given 
 * a bunch of nodes that need to move it determines 
 * who they need to hand off data in terms of ranges.
*/
public class LeaveJoinProtocolImpl implements Runnable
{
    private static Logger logger_ = Logger.getLogger(LeaveJoinProtocolImpl.class);    
    
    /* endpoints that are to be moved. */
    protected EndPoint[] targets_ = new EndPoint[0];
    /* position where they need to be moved */
    protected final Token[] tokens_;
    /* token metadata information */
    protected TokenMetadata tokenMetadata_ = null;

    public LeaveJoinProtocolImpl(EndPoint[] targets, Token[] tokens)
    {
        targets_ = targets;
        tokens_ = tokens;
        tokenMetadata_ = StorageService.instance().getTokenMetadata();
    }

    public void run()
    {  
        try
        {
            logger_.debug("Beginning leave/join process for ...");                                                               
            /* copy the token to endpoint map */
            Map<Token, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
            /* copy the endpoint to token map */
            Map<EndPoint, Token> endpointToTokenMap = tokenMetadata_.cloneEndPointTokenMap();
            
            Set<Token> oldTokens = new HashSet<Token>( tokenToEndPointMap.keySet() );
            Range[] oldRanges = StorageService.instance().getAllRanges(oldTokens);
            logger_.debug("Total number of old ranges " + oldRanges.length);
            /* Calculate the list of nodes that handle the old ranges */
            Map<Range, List<EndPoint>> oldRangeToEndPointMap = StorageService.instance().constructRangeToEndPointMap(oldRanges);
            
            /* Remove the tokens of the nodes leaving the ring */
            Set<Token> tokens = getTokensForLeavingNodes();
            oldTokens.removeAll(tokens);
            Range[] rangesAfterNodesLeave = StorageService.instance().getAllRanges(oldTokens);
            /* Get expanded range to initial range mapping */
            Map<Range, List<Range>> expandedRangeToOldRangeMap = getExpandedRangeToOldRangeMapping(oldRanges, rangesAfterNodesLeave);
            /* add the new token positions to the old tokens set */
            for (Token token : tokens_)
                oldTokens.add(token);
            Range[] rangesAfterNodesJoin = StorageService.instance().getAllRanges(oldTokens);
            /* replace the ranges that were split with the split ranges in the old configuration */
            addSplitRangesToOldConfiguration(oldRangeToEndPointMap, rangesAfterNodesJoin);
            
            /* Re-calculate the new ranges after the new token positions are added */
            Range[] newRanges = StorageService.instance().getAllRanges(oldTokens);
            /* Remove the old locations from tokenToEndPointMap and add the new locations they are moving to */
            for ( int i = 0; i < targets_.length; ++i )
            {
                tokenToEndPointMap.remove( endpointToTokenMap.get(targets_[i]) );
                tokenToEndPointMap.put(tokens_[i], targets_[i]);
            }            
            /* Calculate the list of nodes that handle the new ranges */            
            Map<Range, List<EndPoint>> newRangeToEndPointMap = StorageService.instance().constructRangeToEndPointMap(newRanges, tokenToEndPointMap);
            /* Remove any expanded ranges and replace them with ranges whose aggregate is the expanded range in the new configuration. */
            removeExpandedRangesFromNewConfiguration(newRangeToEndPointMap, expandedRangeToOldRangeMap);
            /* Calculate ranges that need to be sent and from whom to where */
            Map<Range, List<BootstrapSourceTarget>> rangesWithSourceTarget = LeaveJoinProtocolHelper.getRangeSourceTargetInfo(oldRangeToEndPointMap, newRangeToEndPointMap);
            /* For debug purposes only */
            Set<Range> ranges = rangesWithSourceTarget.keySet();
            for ( Range range : ranges )
            {
                System.out.print("RANGE: " + range + ":: ");
                List<BootstrapSourceTarget> infos = rangesWithSourceTarget.get(range);
                for ( BootstrapSourceTarget info : infos )
                {
                    System.out.print(info);
                    System.out.print(" ");
                }
                System.out.println(System.getProperty("line.separator"));
            }
            /* Send messages to respective folks to stream data over to the new nodes being bootstrapped */
            LeaveJoinProtocolHelper.assignWork(rangesWithSourceTarget);
        }
        catch ( Throwable th )
        {
            logger_.warn(LogUtil.throwableToString(th));
        }
    }
    
    /**
     * This method figures out the ranges that have been split and
     * replaces them with the split range.
     * @param oldRangeToEndPointMap old range mapped to their replicas.
     * @param rangesAfterNodesJoin ranges after the nodes have joined at
     *        their respective position.
     */
    private void addSplitRangesToOldConfiguration(Map<Range, List<EndPoint>> oldRangeToEndPointMap, Range[] rangesAfterNodesJoin)
    {
        /* 
         * Find the ranges that are split. Maintain a mapping between
         * the range being split and the list of subranges.
        */                
        Map<Range, List<Range>> splitRanges = LeaveJoinProtocolHelper.getRangeSplitRangeMapping(oldRangeToEndPointMap.keySet().toArray( new Range[0] ), tokens_);
        /* Mapping of split ranges to the list of endpoints responsible for the range */                
        Map<Range, List<EndPoint>> replicasForSplitRanges = new HashMap<Range, List<EndPoint>>();                                
        Set<Range> rangesSplit = splitRanges.keySet();                
        for ( Range splitRange : rangesSplit )
        {
            replicasForSplitRanges.put( splitRange, oldRangeToEndPointMap.get(splitRange) );
        }
        /* Remove the ranges that are split. */
        for ( Range splitRange : rangesSplit )
        {
            oldRangeToEndPointMap.remove(splitRange);
        }
        
        /* Add the subranges of the split range to the map with the same replica set. */
        for ( Range splitRange : rangesSplit )
        {
            List<Range> subRanges = splitRanges.get(splitRange);
            List<EndPoint> replicas = replicasForSplitRanges.get(splitRange);
            for ( Range subRange : subRanges )
            {
                /* Make sure we clone or else we are hammered. */
                oldRangeToEndPointMap.put(subRange, new ArrayList<EndPoint>(replicas));
            }
        }
    }
    
    /**
     * Reset the newRangeToEndPointMap and replace the expanded range
     * with the ranges whose aggregate is the expanded range. This happens
     * only when nodes leave the ring to migrate to a different position.
     * 
     * @param newRangeToEndPointMap all new ranges mapped to the replicas 
     *        responsible for those ranges.
     * @param expandedRangeToOldRangeMap mapping between the expanded ranges
     *        and the ranges whose aggregate is the expanded range.
     */
    private void removeExpandedRangesFromNewConfiguration(Map<Range, List<EndPoint>> newRangeToEndPointMap, Map<Range, List<Range>> expandedRangeToOldRangeMap)
    {
        /* Get the replicas for the expanded ranges */
        Map<Range, List<EndPoint>> replicasForExpandedRanges = new HashMap<Range, List<EndPoint>>();
        Set<Range> expandedRanges = expandedRangeToOldRangeMap.keySet();
        for ( Range expandedRange : expandedRanges )
        {            
            replicasForExpandedRanges.put( expandedRange, newRangeToEndPointMap.get(expandedRange) );
            newRangeToEndPointMap.remove(expandedRange);            
        }
        /* replace the expanded ranges in the newRangeToEndPointMap with the subRanges */
        for ( Range expandedRange : expandedRanges )
        {
            List<Range> subRanges = expandedRangeToOldRangeMap.get(expandedRange);
            List<EndPoint> replicas = replicasForExpandedRanges.get(expandedRange);          
            for ( Range subRange : subRanges )
            {
                newRangeToEndPointMap.put(subRange, new ArrayList<EndPoint>(replicas));
            }
        }        
    }
    
    private Set<Token> getTokensForLeavingNodes()
    {
        Set<Token> tokens = new HashSet<Token>();
        for ( EndPoint target : targets_ )
        {
            tokens.add(tokenMetadata_.getToken(target));
        }        
        return tokens;
    }
    
    /**
     * Here we are removing the nodes that need to leave the
     * ring and trying to calculate what the ranges would look
     * like w/o them. For eg if we remove two nodes A and D from
     * the ring and the order of nodes on the ring is A, B, C
     * and D. When B is removed the range of C is the old range 
     * of C and the old range of B. We want a mapping from old
     * range of B to new range of B. We have 
     * A----B----C----D----E----F----G and we remove b and e
     * then we want a mapping from (a, c] --> (a,b], (b, c] and 
     * (d, f] --> (d, e], (d,f].
     * @param oldRanges ranges with the previous configuration
     * @param newRanges ranges with the target endpoints removed.
     * @return map of expanded range to the list whose aggregate is
     *             the expanded range.
     */
    protected static Map<Range, List<Range>> getExpandedRangeToOldRangeMapping(Range[] oldRanges, Range[] newRanges)
    {
        Map<Range, List<Range>> map = new HashMap<Range, List<Range>>();   
        List<Range> oRanges = new ArrayList<Range>();
        Collections.addAll(oRanges, oldRanges);
        List<Range> nRanges = new ArrayList<Range>();
        Collections.addAll(nRanges, newRanges);
        
        /*
         * Remove the ranges that are the same. 
         * Now we will be left with the expanded 
         * ranges in the nRanges list and the 
         * smaller ranges in the oRanges list. 
        */
        for( Range oRange : oldRanges )
        {            
            boolean bVal = nRanges.remove(oRange);
            if ( bVal )
                oRanges.remove(oRange);
        }
        
        int nSize = nRanges.size();
        int oSize = oRanges.size();
        /*
         * Establish the mapping between expanded ranges
         * to the smaller ranges whose aggregate is the
         * expanded range. 
        */
        for ( int i = 0; i < nSize; ++i )
        {
            Range nRange = nRanges.get(i);
            for ( int j = 0; j < oSize; ++j )
            {
                Range oRange = oRanges.get(j);
                if ( nRange.contains(oRange.right()) )
                {
                    List<Range> smallerRanges = map.get(nRange);
                    if ( smallerRanges == null )
                    {
                        smallerRanges = new ArrayList<Range>();
                        map.put(nRange, smallerRanges);
                    }
                    smallerRanges.add(oRange);
                    continue;
                }
            }
        }
        
        return map;
    }

    public static void main(String[] args) throws Throwable
    {
        StorageService ss = StorageService.instance();
        ss.updateTokenMetadata(new BigIntegerToken("3"), new EndPoint("A", 7000));
        ss.updateTokenMetadata(new BigIntegerToken("6"), new EndPoint("B", 7000));
        ss.updateTokenMetadata(new BigIntegerToken("9"), new EndPoint("C", 7000));
        ss.updateTokenMetadata(new BigIntegerToken("12"), new EndPoint("D", 7000));
        ss.updateTokenMetadata(new BigIntegerToken("15"), new EndPoint("E", 7000));
        ss.updateTokenMetadata(new BigIntegerToken("18"), new EndPoint("F", 7000));
        ss.updateTokenMetadata(new BigIntegerToken("21"), new EndPoint("G", 7000));
        ss.updateTokenMetadata(new BigIntegerToken("24"), new EndPoint("H", 7000));
        
        Runnable runnable = new LeaveJoinProtocolImpl( new EndPoint[]{new EndPoint("C", 7000), new EndPoint("D", 7000)}, new Token[]{new BigIntegerToken("22"), new BigIntegerToken("23")} );
        runnable.run();
    }
}
