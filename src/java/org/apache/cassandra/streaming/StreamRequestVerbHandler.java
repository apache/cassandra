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
 package org.apache.cassandra.streaming;

 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;

/**
 * This verb handler handles the StreamRequestMessage that is sent by
 * the node requesting range transfer.
*/
public class StreamRequestVerbHandler implements IVerbHandler<StreamRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(StreamRequestVerbHandler.class);

    public void doVerb(MessageIn<StreamRequest> message, String id)
    {
        if (logger.isDebugEnabled())
            logger.debug("Received a StreamRequestMessage from {}", message.from);

        StreamRequest srm = message.payload;
        if (logger.isDebugEnabled())
            logger.debug(srm.toString());

        StreamOutSession session = StreamOutSession.create(srm.table, message.from, srm.sessionId);
        StreamOut.transferRanges(session, srm.columnFamilies, srm.ranges, srm.type);
    }
}

