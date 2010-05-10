/**
 *
 */
package org.apache.cassandra.service;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractRackAwareSnitch;
import org.apache.cassandra.net.Message;

/**
 * This class will block for the replication factor which is
 * provided in the input map. it will block till we recive response from
 * n nodes in each of our data centers.
 */
public class DatacenterSyncWriteResponseHandler extends WriteResponseHandler
{
    private final Map<String, Integer> dcResponses = new HashMap<String, Integer>();
    private final Map<String, Integer> responseCounts;
    private final AbstractRackAwareSnitch endpointSnitch;

    public DatacenterSyncWriteResponseHandler(Map<String, Integer> responseCounts, String table)
    {
        // Response is been managed by the map so make it 1 for the superclass.
        super(1, table);
        this.responseCounts = responseCounts;
        endpointSnitch = (AbstractRackAwareSnitch) DatabaseDescriptor.getEndpointSnitch();
    }

    @Override
    // synchronized for the benefit of dcResponses and responseCounts.  "responses" itself
    // is inherited from WRH and is concurrent.
    // TODO can we use concurrent structures instead?
    public synchronized void response(Message message)
    {
        try
        {
            String dataCenter = endpointSnitch.getDatacenter(message.getFrom());
            Object blockFor = responseCounts.get(dataCenter);
            // If this DC needs to be blocked then do the below.
            if (blockFor != null)
            {
                Integer quorumCount = dcResponses.get(dataCenter);
                if (quorumCount == null)
                {
                    // Intialize and recognize the first response
                    dcResponses.put(dataCenter, 1);
                }
                else if ((Integer) blockFor > quorumCount)
                {
                    // recognize the consequtive responses.
                    dcResponses.put(dataCenter, quorumCount + 1);
                }
                else
                {
                    // No need to wait on it anymore so remove it.
                    responseCounts.remove(dataCenter);
                }
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        responses.add(message);
        // If done then the response count will be empty
        if (responseCounts.isEmpty())
        {
            condition.signal();
        }
    }
}
