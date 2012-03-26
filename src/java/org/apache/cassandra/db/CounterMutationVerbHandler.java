/*
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
package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;

public class CounterMutationVerbHandler implements IVerbHandler<CounterMutation>
{
    private static final Logger logger = LoggerFactory.getLogger(CounterMutationVerbHandler.class);

    public void doVerb(MessageIn<CounterMutation> message, String id)
    {
        try
        {
            CounterMutation cm = message.payload;
            if (logger.isDebugEnabled())
              logger.debug("Applying forwarded " + cm);

            String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
            StorageProxy.applyCounterMutationOnLeader(cm, localDataCenter).get();
            WriteResponse response = new WriteResponse(cm.getTable(), cm.key(), true);
            MessagingService.instance().sendReply(response.createMessage(), id, message.from);
        }
        catch (UnavailableException e)
        {
            // We check for UnavailableException in the coordinator not. It is
            // hence reasonable to let the coordinator timeout in the very
            // unlikely case we arrive here
        }
        catch (TimeoutException e)
        {
            // The coordinator node will have timeout itself so we let that goes
        }
        catch (IOException e)
        {
            logger.error("Error in counter mutation", e);
        }
    }
}
