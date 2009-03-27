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
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

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
        IPartitioner p = StorageService.getPartitioner();
        Token token = p.getTokenFactory().fromString(args[1]);
        String file = args[2];
        
        String[] ipPortPair = ipPort.split(":");
        EndPoint target = new EndPoint(ipPortPair[0], Integer.valueOf(ipPortPair[1]));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        Token.serializer().serialize(token, dos);

        /* Construct the token update message to be sent */
        Message tokenUpdateMessage = new Message( new EndPoint(FBUtilities.getHostName(), port_), "", StorageService.tokenVerbHandler_, new Object[]{bos.toByteArray()} );
        
        BufferedReader bufReader = new BufferedReader( new InputStreamReader( new FileInputStream(file) ) );
        String line = null;
       
        while ( ( line = bufReader.readLine() ) != null )
        {
            String[] nodeTokenPair = line.split(" ");
            /* Add the node and the token pair into the header of this message. */
            Token nodeToken = p.getTokenFactory().fromString(nodeTokenPair[1]);
            tokenUpdateMessage.addHeader(nodeTokenPair[0], p.getTokenFactory().toByteArray(nodeToken));
        }
        
        System.out.println("Sending a token update message to " + target);
        MessagingService.getMessagingInstance().sendOneWay(tokenUpdateMessage, target);
        Thread.sleep(TokenUpdater.waitTime_);
        System.out.println("Done sending the update message");
    }

}
