package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

/** for streaming data from other nodes in to this one */
public class StreamIn
{
    private static Logger logger = Logger.getLogger(StreamOut.class);

    /**
     * Request ranges to be transferred from source to local node
     */
    public static void requestRanges(InetAddress source, String tableName, Collection<Range> ranges)
    {
        if (logger.isDebugEnabled())
            logger.debug("Requesting from " + source + " ranges " + StringUtils.join(ranges, ", "));
        StreamRequestMetadata streamRequestMetadata = new StreamRequestMetadata(FBUtilities.getLocalAddress(), ranges, tableName);
        Message message = StreamRequestMessage.makeStreamRequestMessage(new StreamRequestMessage(streamRequestMetadata));
        MessagingService.instance.sendOneWay(message, source);
    }
}
