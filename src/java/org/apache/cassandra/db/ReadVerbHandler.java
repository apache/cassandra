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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import java.net.InetAddress;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.log4j.Logger;

public class ReadVerbHandler implements IVerbHandler
{
    protected static class ReadContext
    {
        protected ByteArrayInputStream bufIn_;
        protected DataOutputBuffer bufOut_ = new DataOutputBuffer();
    }

    private static Logger logger_ = Logger.getLogger( ReadVerbHandler.class );
    /* We use this so that we can reuse readcontext objects */
    private static ThreadLocal<ReadVerbHandler.ReadContext> tls_ = new InheritableThreadLocal<ReadVerbHandler.ReadContext>();

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
        readCtx.bufIn_ = new ByteArrayInputStream(body);

        try
        {
            if (StorageService.instance.isBootstrapMode())
            {
                /* Don't service reads! */
                throw new RuntimeException("Cannot service reads while bootstrapping!");
            }
            ReadCommand command = ReadCommand.serializer().deserialize(new DataInputStream(readCtx.bufIn_));
            Table table = Table.open(command.table);
            Row row = command.getRow(table);
            ReadResponse readResponse;
            if (command.isDigestQuery())
            {
                if (logger_.isDebugEnabled())
                    logger_.debug("digest is " + FBUtilities.bytesToHex(ColumnFamily.digest(row.cf)));
                readResponse = new ReadResponse(ColumnFamily.digest(row.cf));
            }
            else
            {
                readResponse = new ReadResponse(row);
            }
            readResponse.setIsDigestQuery(command.isDigestQuery());
            /* serialize the ReadResponseMessage. */
            readCtx.bufOut_.reset();

            ReadResponse.serializer().serialize(readResponse, readCtx.bufOut_);

            byte[] bytes = new byte[readCtx.bufOut_.getLength()];
            System.arraycopy(readCtx.bufOut_.getData(), 0, bytes, 0, bytes.length);

            Message response = message.getReply(FBUtilities.getLocalAddress(), bytes);
            if (logger_.isDebugEnabled())
              logger_.debug("Read key " + command.key + "; sending response to " + message.getMessageId() + "@" + message.getFrom());
            MessagingService.instance.sendOneWay(response, message.getFrom());

            /* Do read repair if header of the message says so */
            if (message.getHeader(ReadCommand.DO_REPAIR) != null)
            {
                List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(command.table, command.key);
                /* Remove the local storage endpoint from the list. */
                endpoints.remove(FBUtilities.getLocalAddress());
                if (endpoints.size() > 0)
                    StorageService.instance.doConsistencyCheck(row, endpoints, command);
            }
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
