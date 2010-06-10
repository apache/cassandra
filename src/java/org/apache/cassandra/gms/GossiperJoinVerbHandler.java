package org.apache.cassandra.gms;

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
