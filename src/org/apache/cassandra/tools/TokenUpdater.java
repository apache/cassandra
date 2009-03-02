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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.io.*;
import org.apache.cassandra.utils.*;

public class TokenUpdater
{
    private static final int port_ = 7000;
    private static final long waitTime_ = 10000;
    
    public static void main(String[] args) throws Throwable
    {
        if ( args.length != 3 )
        {
            System.out.println("Usage : java com.facebook.infrastructure.tools.TokenUpdater <ip:port> <token> <file containing node token info>");
            System.exit(1);
        }
        
        String ipPort = args[0];
        String token = args[1];
        String file = args[2];
        
        String[] ipPortPair = ipPort.split(":");
        EndPoint target = new EndPoint(ipPortPair[0], Integer.valueOf(ipPortPair[1]));
        TokenInfoMessage tiMessage = new TokenInfoMessage( target, new BigInteger(token) );
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        TokenInfoMessage.serializer().serialize(tiMessage, dos);
        /* Construct the token update message to be sent */
        Message tokenUpdateMessage = new Message( new EndPoint(FBUtilities.getHostName(), port_), "", StorageService.tokenVerbHandler_, new Object[]{bos.toByteArray()} );
        
        BufferedReader bufReader = new BufferedReader( new InputStreamReader( new FileInputStream(file) ) );
        String line = null;
       
        while ( ( line = bufReader.readLine() ) != null )
        {
            String[] nodeTokenPair = line.split(" ");
            /* Add the node and the token pair into the header of this message. */
            BigInteger nodeToken = new BigInteger(nodeTokenPair[1]);
            tokenUpdateMessage.addHeader(nodeTokenPair[0], nodeToken.toByteArray());
        }
        
        System.out.println("Sending a token update message to " + target);
        MessagingService.getMessagingInstance().sendOneWay(tokenUpdateMessage, target);
        Thread.sleep(TokenUpdater.waitTime_);
        System.out.println("Done sending the update message");
    }
    
    public static class TokenInfoMessage implements Serializable
    {
        private static ICompactSerializer<TokenInfoMessage> serializer_;
        private static AtomicInteger idGen_ = new AtomicInteger(0);
        
        static
        {
            serializer_ = new TokenInfoMessageSerializer();            
        }
        
        static ICompactSerializer<TokenInfoMessage> serializer()
        {
            return serializer_;
        }

        private EndPoint target_;
        private BigInteger token_;
        
        TokenInfoMessage(EndPoint target, BigInteger token)
        {
            target_ = target;
            token_ = token;
        }
        
        EndPoint getTarget()
        {
            return target_;
        }
        
        BigInteger getToken()
        {
            return token_;
        }
    }
    
    public static class TokenInfoMessageSerializer implements ICompactSerializer<TokenInfoMessage>
    {
        public void serialize(TokenInfoMessage tiMessage, DataOutputStream dos) throws IOException
        {
            byte[] node = EndPoint.toBytes( tiMessage.getTarget() );
            dos.writeInt(node.length);
            dos.write(node);
            
            byte[] token = tiMessage.getToken().toByteArray();
            dos.writeInt( token.length );
            dos.write(token);
        }
        
        public TokenInfoMessage deserialize(DataInputStream dis) throws IOException
        {
            byte[] target = new byte[dis.readInt()];
            dis.readFully(target);
            
            byte[] token = new byte[dis.readInt()];
            dis.readFully(token);
            
            return new TokenInfoMessage(EndPoint.fromBytes(target), new BigInteger(token));
        }
    }
}
