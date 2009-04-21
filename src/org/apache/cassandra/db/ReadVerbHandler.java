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
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

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
            ReadCommand readCommand = ReadCommand.serializer().deserialize(readCtx.bufIn_);
            Table table = Table.open(readCommand.table);
            Row row = null;
            long start = System.currentTimeMillis();
            row = readCommand.getRow(table);
            logger_.info("getRow()  TIME: " + (System.currentTimeMillis() - start) + " ms.");
            start = System.currentTimeMillis();
            ReadResponse readResponse = null;
            if(readCommand.isDigestQuery())
            {
                readResponse = new ReadResponse(table.getTableName(), row.digest());
            }
            else
            {
                readResponse = new ReadResponse(table.getTableName(), row);
            }
            readResponse.setIsDigestQuery(readCommand.isDigestQuery());
            /* serialize the ReadResponseMessage. */
            readCtx.bufOut_.reset();

            start = System.currentTimeMillis();
            ReadResponse.serializer().serialize(readResponse, readCtx.bufOut_);
            logger_.info("serialize  TIME: " + (System.currentTimeMillis() - start) + " ms.");

            byte[] bytes = new byte[readCtx.bufOut_.getLength()];
            start = System.currentTimeMillis();
            System.arraycopy(readCtx.bufOut_.getData(), 0, bytes, 0, bytes.length);
            logger_.info("copy  TIME: " + (System.currentTimeMillis() - start) + " ms.");

            Message response = message.getReply( StorageService.getLocalStorageEndPoint(), new Object[]{bytes} );
            MessagingService.getMessagingInstance().sendOneWay(response, message.getFrom());
            logger_.info("ReadVerbHandler  TIME 2: " + (System.currentTimeMillis() - start) + " ms.");
            
            /* Do read repair if header of the message says so */
            String repair = new String( message.getHeader(ReadCommand.DO_REPAIR) );
            if ( repair.equals( ReadCommand.DO_REPAIR) )
                doReadRepair(row, readCommand);
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
    
    private void doReadRepair(Row row, ReadCommand readCommand)
    {
        if ( DatabaseDescriptor.getConsistencyCheck() )
        {
            List<EndPoint> endpoints = StorageService.instance().getNLiveStorageEndPoint(readCommand.key);
            /* Remove the local storage endpoint from the list. */ 
            endpoints.remove( StorageService.getLocalStorageEndPoint() );
            
            if(readCommand.columnNames.size() == 0)
            {
                if( readCommand.start >= 0 && readCommand.count < Integer.MAX_VALUE)
                {                
                    StorageService.instance().doConsistencyCheck(row, endpoints, readCommand.columnFamilyColumn, readCommand.start, readCommand.count);
                }
                
                if( readCommand.sinceTimestamp > 0)
                {                    
                    StorageService.instance().doConsistencyCheck(row, endpoints, readCommand.columnFamilyColumn, readCommand.sinceTimestamp);
                }                
            }
            else
            {
                StorageService.instance().doConsistencyCheck(row, endpoints, readCommand.columnFamilyColumn, readCommand.columnNames);
            }
        }
    }     
}
