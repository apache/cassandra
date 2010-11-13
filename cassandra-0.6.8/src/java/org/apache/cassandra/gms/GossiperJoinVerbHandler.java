package org.apache.cassandra.gms;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;

public class GossiperJoinVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger( GossiperJoinVerbHandler.class);

    public void doVerb(Message message)
    {
        InetAddress from = message.getFrom();
        if (logger_.isDebugEnabled())
          logger_.debug("Received a JoinMessage from " + from);

        byte[] bytes = message.getMessageBody();
        DataInputStream dis = new DataInputStream( new ByteArrayInputStream(bytes) );

        JoinMessage joinMessage;
        try
        {
            joinMessage = JoinMessage.serializer().deserialize(dis);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        if ( joinMessage.clusterId_.equals( DatabaseDescriptor.getClusterName() ) )
        {
            Gossiper.instance.join(from);
        }
        else
        {
            logger_.warn("ClusterName mismatch from " + from + " " + joinMessage.clusterId_  + "!=" + DatabaseDescriptor.getClusterName());
        }
    }
}
