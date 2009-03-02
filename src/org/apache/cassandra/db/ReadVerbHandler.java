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
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.cassandra.continuations.Suspendable;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.net.*;
import org.apache.cassandra.utils.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ReadVerbHandler implements IVerbHandler
{
    private static class ReadContext
    {
        protected DataInputBuffer bufIn_ = new DataInputBuffer();
        protected DataOutputBuffer bufOut_ = new DataOutputBuffer();
    }

    private static Logger logger_ = Logger.getLogger( ReadVerbHandler.class );
    /* We use this so that we can reuse the same row mutation context for the mutation. */
    private static ThreadLocal<ReadContext> tls_ = new InheritableThreadLocal<ReadContext>();

    public void doVerb(Message message)
    {
        byte[] body = (byte[])message.getMessageBody()[0];
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
            ReadMessage readMessage = ReadMessage.serializer().deserialize(readCtx.bufIn_);
            Table table = Table.open(readMessage.table());
            Row row = null;
            long start = System.currentTimeMillis();
            if( readMessage.columnFamily_column() == null )
            	row = table.get(readMessage.key());
            else
            {
            	if(readMessage.getColumnNames().size() == 0)
            	{
	            	if(readMessage.count() > 0 && readMessage.start() >= 0)
	            		row = table.getRow(readMessage.key(), readMessage.columnFamily_column(), readMessage.start(), readMessage.count());
	            	else
	            		row = table.getRow(readMessage.key(), readMessage.columnFamily_column());
            	}
            	else
            	{
            		row = table.getRow(readMessage.key(), readMessage.columnFamily_column(), readMessage.getColumnNames());            		
            	}
            }              
            logger_.info("getRow()  TIME: " + (System.currentTimeMillis() - start) + " ms.");
            start = System.currentTimeMillis();
            ReadResponseMessage readResponseMessage = null;
            if(readMessage.isDigestQuery())
            {
                readResponseMessage = new ReadResponseMessage(table.getTableName(), row.digest());

            }
            else
            {
                readResponseMessage = new ReadResponseMessage(table.getTableName(), row);
            }
            readResponseMessage.setIsDigestQuery(readMessage.isDigestQuery());
            /* serialize the ReadResponseMessage. */
            readCtx.bufOut_.reset();

            start = System.currentTimeMillis();
            ReadResponseMessage.serializer().serialize(readResponseMessage, readCtx.bufOut_);
            logger_.info("serialize  TIME: " + (System.currentTimeMillis() - start) + " ms.");

            byte[] bytes = new byte[readCtx.bufOut_.getLength()];
            start = System.currentTimeMillis();
            System.arraycopy(readCtx.bufOut_.getData(), 0, bytes, 0, bytes.length);
            logger_.info("copy  TIME: " + (System.currentTimeMillis() - start) + " ms.");

            Message response = message.getReply( StorageService.getLocalStorageEndPoint(), new Object[]{bytes} );
            MessagingService.getMessagingInstance().sendOneWay(response, message.getFrom());
            logger_.info("ReadVerbHandler  TIME 2: " + (System.currentTimeMillis() - start)
                    + " ms.");
        }
        catch ( IOException ex)
        {
            logger_.info( LogUtil.throwableToString(ex) );
        }
        catch ( ColumnFamilyNotDefinedException ex)
        {
            logger_.info( LogUtil.throwableToString(ex) );
        }
    }
}
