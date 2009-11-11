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

import java.io.File;
import java.io.IOException;
import java.io.IOError;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.Streaming;

import java.net.InetAddress;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.io.StreamContextManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StreamManager;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;

 /**
 * This verb handler handles the BootstrapMetadataMessage that is sent
 * by the leader to the nodes that are responsible for handing off data. 
*/
public class BootstrapMetadataVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(BootstrapMetadataVerbHandler.class);
    
    public void doVerb(Message message)
    {
        if (logger_.isDebugEnabled())
          logger_.debug("Received a BootstrapMetadataMessage from " + message.getFrom());
        
        /* Cannot bootstrap another node if I'm in bootstrap mode myself! */
        assert !StorageService.instance().isBootstrapMode();
        
        byte[] body = message.getMessageBody();
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(body, body.length);
        try
        {
            BootstrapMetadataMessage bsMetadataMessage = BootstrapMetadataMessage.serializer().deserialize(bufIn);
            BootstrapMetadata[] bsMetadata = bsMetadataMessage.bsMetadata_;

            for (BootstrapMetadata bsmd : bsMetadata)
            {
                if (logger_.isDebugEnabled())
                    logger_.debug(bsmd.toString());
                Streaming.transferRanges(bsmd.target_, bsmd.ranges_, null);
            }
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }
}

