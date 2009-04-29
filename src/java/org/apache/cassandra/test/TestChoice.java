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

package org.apache.cassandra.test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.EndPointSnitch;
import org.apache.cassandra.locator.IEndPointSnitch;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

public class TestChoice
{
    private static final Logger logger_ = Logger.getLogger(TestChoice.class);
    private Set<EndPoint> allNodes_;
    private Map<EndPoint, List<EndPoint>> nodeToReplicaMap_ = new HashMap<EndPoint, List<EndPoint>>();
    
    public TestChoice(Set<EndPoint> allNodes)
    {
        allNodes_ = new HashSet<EndPoint>(allNodes);
    }
    
    public void assignReplicas()
    {
        IEndPointSnitch snitch = new EndPointSnitch();
        Set<EndPoint> allNodes = new HashSet<EndPoint>(allNodes_);
        Map<EndPoint, Integer> nOccurences = new HashMap<EndPoint, Integer>();
        
        for ( EndPoint node : allNodes_ )
        {
            nOccurences.put(node, 1);
        }
        
        for ( EndPoint node : allNodes_ )
        {
            allNodes.remove(node);
            for ( EndPoint choice : allNodes )
            {
                List<EndPoint> replicasChosen = nodeToReplicaMap_.get(node);
                if ( replicasChosen == null || replicasChosen.size() < DatabaseDescriptor.getReplicationFactor() - 1 )
                {
                    try
                    {
                        if ( !snitch.isInSameDataCenter(node, choice) )
                        {
                            if ( replicasChosen == null )
                            {
                                replicasChosen = new ArrayList<EndPoint>();
                                nodeToReplicaMap_.put(node, replicasChosen);
                            }
                            int nOccurence = nOccurences.get(choice);
                            if ( nOccurence < DatabaseDescriptor.getReplicationFactor() )
                            {
                                nOccurences.put(choice, ++nOccurence);
                                replicasChosen.add(choice);
                            }                            
                        }
                    }
                    catch ( UnknownHostException ex )
                    {
                        ex.printStackTrace();
                    }
                }
                else
                {
                    allNodes.add(node);
                    break;
                }
            }
        }
        
        
        Set<EndPoint> nodes = nodeToReplicaMap_.keySet();
        for ( EndPoint node : nodes )
        {
            List<EndPoint> replicas = nodeToReplicaMap_.get(node);
            StringBuilder sb = new StringBuilder("");
            for ( EndPoint replica : replicas )
            {
                sb.append(replica);
                sb.append(", ");
            }
            System.out.println(node + " ---> " + sb.toString() );
        }
    }
}
