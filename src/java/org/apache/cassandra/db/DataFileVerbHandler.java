/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.log4j.Logger;


public class DataFileVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger( DataFileVerbHandler.class );
    
    public void doVerb(Message message)
    {        
        byte[] bytes = message.getMessageBody();
        String table = new String(bytes);
        logger_.info("**** Received a request from " + message.getFrom());
        
        try
        {
            List<SSTableReader> ssTables = Table.open(table).getAllSSTablesOnDisk();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeInt(ssTables.size());
            for (SSTableReader sstable : ssTables)
            {
                dos.writeUTF(sstable.getFilename());
            }
            Message response = message.getReply(FBUtilities.getLocalAddress(), bos.toByteArray());
            MessagingService.instance().sendOneWay(response, message.getFrom());
        }
        catch (IOException ex)
        {
            logger_.error("Error listing data files", ex);
        }
    }
}
