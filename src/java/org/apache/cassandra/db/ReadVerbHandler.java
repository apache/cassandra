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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class ReadVerbHandler implements IVerbHandler
{
    protected static class ReadContext
    {
        protected ByteArrayInputStream bufIn_;
        protected DataOutputBuffer bufOut_ = new DataOutputBuffer();
    }

    private static Logger logger_ = LoggerFactory.getLogger( ReadVerbHandler.class );
    /* We use this so that we can reuse readcontext objects */
    private static ThreadLocal<ReadVerbHandler.ReadContext> tls_ = new InheritableThreadLocal<ReadVerbHandler.ReadContext>();

    public void doVerb(Message message, String id)
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
            ReadCommand command = ReadCommand.serializer().deserialize(new DataInputStream(readCtx.bufIn_), message.getVersion());
            Table table = Table.open(command.table);
            Row row = command.getRow(table);
            ReadResponse readResponse = getResponse(command, row);
            /* serialize the ReadResponseMessage. */
            readCtx.bufOut_.reset();

            ReadResponse.serializer().serialize(readResponse, readCtx.bufOut_, message.getVersion());

            byte[] bytes = new byte[readCtx.bufOut_.getLength()];
            System.arraycopy(readCtx.bufOut_.getData(), 0, bytes, 0, bytes.length);

            Message response = message.getReply(FBUtilities.getLocalAddress(), bytes, message.getVersion());
            if (logger_.isDebugEnabled())
              logger_.debug(String.format("Read key %s; sending response to %s@%s",
                                          ByteBufferUtil.bytesToHex(command.key), id, message.getFrom()));
            MessagingService.instance().sendReply(response, id, message.getFrom());
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public static ReadResponse getResponse(ReadCommand command, Row row)
    {
        if (command.isDigestQuery())
        {
            if (logger_.isDebugEnabled())
                logger_.debug("digest is " + ByteBufferUtil.bytesToHex(ColumnFamily.digest(row.cf)));
            return new ReadResponse(ColumnFamily.digest(row.cf));
        }
        else
        {
            return new ReadResponse(row);
        }
    }
}
