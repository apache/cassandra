package org.apache.cassandra.gms;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

public class GossipDigestAck2VerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(GossipDigestAck2VerbHandler.class);

    public void doVerb(Message message)
    {
        InetAddress from = message.getFrom();
        if (logger_.isTraceEnabled())
            logger_.trace("Received a GossipDigestAck2Message from " + from);

        byte[] bytes = message.getMessageBody();
        DataInputStream dis = new DataInputStream( new ByteArrayInputStream(bytes) );
        GossipDigestAck2Message gDigestAck2Message;
        try
        {
            gDigestAck2Message = GossipDigestAck2Message.serializer().deserialize(dis);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        Map<InetAddress, EndPointState> remoteEpStateMap = gDigestAck2Message.getEndPointStateMap();
        /* Notify the Failure Detector */
        Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
        Gossiper.instance.applyStateLocally(remoteEpStateMap);
    }
}
