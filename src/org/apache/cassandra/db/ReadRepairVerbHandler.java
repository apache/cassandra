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

import java.io.*;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.service.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.net.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ReadRepairVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(ReadRepairVerbHandler.class);    
    
    public void doVerb(Message message)
    {          
        byte[] body = (byte[])message.getMessageBody()[0];
        DataInputBuffer buffer = new DataInputBuffer();
        buffer.reset(body, body.length);        
        
        try
        {
            RowMutationMessage rmMsg = RowMutationMessage.serializer().deserialize(buffer);
            RowMutation rm = rmMsg.getRowMutation();
            rm.apply();                                   
        }
        catch( ColumnFamilyNotDefinedException ex )
        {
            logger_.debug(LogUtil.throwableToString(ex));
        }
        catch ( IOException e )
        {
            logger_.debug(LogUtil.throwableToString(e));            
        }        
    }
}
