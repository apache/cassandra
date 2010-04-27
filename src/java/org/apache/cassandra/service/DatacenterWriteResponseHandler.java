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


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractRackAwareSnitch;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This class will basically will block for the replication factor which is
 * provided in the input map. it will block till we recive response from (DC, n)
 * nodes.
 */
public class DatacenterWriteResponseHandler extends WriteResponseHandler
{
    private final AtomicInteger blockFor;
    private final AbstractRackAwareSnitch endpointsnitch;
    private final InetAddress localEndpoint;

    public DatacenterWriteResponseHandler(int blockFor, String table)
    {
        // Response is been managed by the map so the waitlist size really doesnt matter.
        super(blockFor, table);
        this.blockFor = new AtomicInteger(blockFor);
        endpointsnitch = (AbstractRackAwareSnitch) DatabaseDescriptor.getEndpointSnitch();
        localEndpoint = FBUtilities.getLocalAddress();
    }

    @Override
    public void response(Message message)
    {
        //Is optimal to check if same datacenter than comparing Arrays.
        int b = -1;
        try
        {
            if (endpointsnitch.getDatacenter(localEndpoint).equals(endpointsnitch.getDatacenter(message.getFrom())))
            {
                b = blockFor.decrementAndGet();
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        responses.add(message);
        if (b == 0)
        {
            //Singnal when Quorum is recived.
            condition.signal();
        }
        if (logger.isDebugEnabled())
            logger.debug("Processed Message: " + message.toString());
    }
}
