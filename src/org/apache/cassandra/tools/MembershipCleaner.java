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

package org.apache.cassandra.tools;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class MembershipCleaner
{
    private static final int port_ = 7000;
    private static final long waitTime_ = 10000;
    
    public static void main(String[] args) throws Throwable
    {
        if ( args.length != 3 )
        {
            System.out.println("Usage : java com.facebook.infrastructure.tools.MembershipCleaner " +
                    "<ip:port to send the message> " +
                    "<node which needs to be removed> " +
                    "<file containing all nodes in the cluster>");
            System.exit(1);
        }
        
        String ipPort = args[0];
        String node = args[1];
        String file = args[2];
        
        String[] ipPortPair = ipPort.split(":");
        EndPoint target = new EndPoint(ipPortPair[0], Integer.valueOf(ipPortPair[1]));
        MembershipCleanerMessage mcMessage = new MembershipCleanerMessage(node);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        MembershipCleanerMessage.serializer().serialize(mcMessage, dos);
        /* Construct the token update message to be sent */
        Message mbrshipCleanerMessage = new Message( new EndPoint(FBUtilities.getHostName(), port_), "", StorageService.mbrshipCleanerVerbHandler_, new Object[]{bos.toByteArray()} );
        
        BufferedReader bufReader = new BufferedReader( new InputStreamReader( new FileInputStream(file) ) );
        String line = null;
       
        while ( ( line = bufReader.readLine() ) != null )
        {            
            mbrshipCleanerMessage.addHeader(line, line.getBytes());
        }
        
        System.out.println("Sending a membership clean message to " + target);
        MessagingService.getMessagingInstance().sendOneWay(mbrshipCleanerMessage, target);
        Thread.sleep(MembershipCleaner.waitTime_);
        System.out.println("Done sending the update message");
    }
    
    public static class MembershipCleanerMessage implements Serializable
    {
        private static ICompactSerializer<MembershipCleanerMessage> serializer_;
        private static AtomicInteger idGen_ = new AtomicInteger(0);
        
        static
        {
            serializer_ = new MembershipCleanerMessageSerializer();            
        }
        
        static ICompactSerializer<MembershipCleanerMessage> serializer()
        {
            return serializer_;
        }

        private String target_;
        
        MembershipCleanerMessage(String target)
        {
            target_ = target;        
        }
        
        String getTarget()
        {
            return target_;
        }
    }
    
    public static class MembershipCleanerMessageSerializer implements ICompactSerializer<MembershipCleanerMessage>
    {
        public void serialize(MembershipCleanerMessage mcMessage, DataOutputStream dos) throws IOException
        {            
            dos.writeUTF(mcMessage.getTarget() );                      
        }
        
        public MembershipCleanerMessage deserialize(DataInputStream dis) throws IOException
        {            
            return new MembershipCleanerMessage(dis.readUTF());
        }
    }
}
