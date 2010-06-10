package org.apache.cassandra.streaming;
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
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

/** for streaming data from other nodes in to this one */
public class StreamIn
{
    private static Logger logger = LoggerFactory.getLogger(StreamOut.class);

    /**
     * Request ranges to be transferred from source to local node
     */
    public static void requestRanges(InetAddress source, String tableName, Collection<Range> ranges)
    {
        if (logger.isDebugEnabled())
            logger.debug("Requesting from " + source + " ranges " + StringUtils.join(ranges, ", "));
        StreamInManager.initContect(source);
        StreamRequestMetadata streamRequestMetadata = new StreamRequestMetadata(FBUtilities.getLocalAddress(), ranges, tableName);
        Message message = StreamRequestMessage.makeStreamRequestMessage(new StreamRequestMessage(streamRequestMetadata));
        MessagingService.instance.sendOneWay(message, source);
    }
}
