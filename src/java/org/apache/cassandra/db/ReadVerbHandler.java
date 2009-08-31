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

package org.apache.cassandra.db;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

import org.apache.log4j.Logger;

public class ReadVerbHandler implements IVerbHandler
{
    protected static class ReadContext
    {
        protected DataInputBuffer bufIn_ = new DataInputBuffer();
        protected DataOutputBuffer bufOut_ = new DataOutputBuffer();
    }

    private static Logger logger_ = Logger.getLogger( ReadVerbHandler.class );
    /* We use this so that we can reuse the same row mutation context for the mutation. */
    private static ThreadLocal<ReadVerbHandler.ReadContext> tls_ = new InheritableThreadLocal<ReadVerbHandler.ReadContext>();
    
    protected static ReadVerbHandler.ReadContext getCurrentReadContext()
    {
        return tls_.get();
    }
    
    protected static void setCurrentReadContext(ReadVerbHandler.ReadContext readContext)
    {
        tls_.set(readContext);
    }

    public void doVerb(Message message)
    {
        byte[] body = message.getMessageBody();
        /* Obtain a Read Context from TLS */
        ReadContext readCtx = tls_.get();
        if ( readCtx == null )
        {
            readCtx = new ReadContext();
            tls_.set(readCtx);
        }
        readCtx.bufIn_.reset(body, body.length);

        try
        {
            if (StorageService.instance().isBootstrapMode())
            {
                /* Don't service reads! */
                throw new RuntimeException("Cannot service reads while bootstrapping!");
            }
            ReadCommand readCommand = ReadCommand.serializer().deserialize(readCtx.bufIn_);
            Table table = Table.open(readCommand.table);
            Row row = null;
            row = readCommand.getRow(table);
            ReadResponse readResponse = null;
            if (readCommand.isDigestQuery())
            {
                readResponse = new ReadResponse(row.digest());
            }
            else
            {
                readResponse = new ReadResponse(row);
            }
            readResponse.setIsDigestQuery(readCommand.isDigestQuery());
            /* serialize the ReadResponseMessage. */
            readCtx.bufOut_.reset();

            ReadResponse.serializer().serialize(readResponse, readCtx.bufOut_);

            byte[] bytes = new byte[readCtx.bufOut_.getLength()];
            System.arraycopy(readCtx.bufOut_.getData(), 0, bytes, 0, bytes.length);

            Message response = message.getReply(StorageService.getLocalStorageEndPoint(), bytes);
            if (logger_.isDebugEnabled())
              logger_.debug("Read key " + readCommand.key + "; sending response to " + message.getMessageId() + "@" + message.getFrom());
            MessagingService.getMessagingInstance().sendOneWay(response, message.getFrom());

            /* Do read repair if header of the message says so */
            if (message.getHeader(ReadCommand.DO_REPAIR) != null)
            {
                doReadRepair(row, readCommand);
            }
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }
    
    private void doReadRepair(Row row, ReadCommand readCommand)
    {
        List<EndPoint> endpoints = StorageService.instance().getLiveReadStorageEndPoints(readCommand.key);
        /* Remove the local storage endpoint from the list. */ 
        endpoints.remove( StorageService.getLocalStorageEndPoint() );
            
        if (endpoints.size() > 0 && DatabaseDescriptor.getConsistencyCheck())
            StorageService.instance().doConsistencyCheck(row, endpoints, readCommand);
    }     
}
