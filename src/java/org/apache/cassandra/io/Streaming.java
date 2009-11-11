package org.apache.cassandra.io;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.File;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.BootstrapInitiateMessage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.net.io.StreamContextManager;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StreamManager;

public class Streaming
{
    private static Logger logger = Logger.getLogger(Streaming.class);

    /*
     * This method needs to figure out the files on disk
     * locally for each range and then stream them using
     * the Bootstrap protocol to the target endpoint.
    */
    public static void transferRanges(InetAddress target, Collection<Range> ranges) throws IOException
    {
        assert ranges.size() > 0;

        if (logger.isDebugEnabled())
            logger.debug("Beginning transfer process to " + target + " for ranges " + StringUtils.join(ranges, ", "));

        /*
         * (1) First we dump all the memtables to disk.
         * (2) Run a version of compaction which will basically
         *     put the keys in the range specified into a directory
         *     named as per the endpoint it is destined for inside the
         *     bootstrap directory.
         * (3) Handoff the data.
        */
        List<String> tables = DatabaseDescriptor.getTables();
        for (String tName : tables)
        {
            Table table = Table.open(tName);
            if (logger.isDebugEnabled())
              logger.debug("Flushing memtables ...");
            table.flush(false);
            if (logger.isDebugEnabled())
              logger.debug("Performing anticompaction ...");
            /* Get the list of files that need to be streamed */
            List<String> fileList = new ArrayList<String>();
            for (SSTableReader sstable : table.forceAntiCompaction(ranges, target))
            {
                fileList.addAll(sstable.getAllFilenames());
            }
            transferOneTable(target, fileList, tName); // also deletes the file, so no further cleanup needed
        }
    }

    /**
     * Stream the files in the bootstrap directory over to the
     * node being bootstrapped.
    */
    private static void transferOneTable(InetAddress target, List<String> fileList, String table) throws IOException
    {
        if (fileList.isEmpty())
            return;

        StreamContextManager.StreamContext[] streamContexts = new StreamContextManager.StreamContext[fileList.size()];
        int i = 0;
        for (String filename : fileList)
        {
            File file = new File(filename);
            streamContexts[i++] = new StreamContextManager.StreamContext(file.getAbsolutePath(), file.length(), table);
            if (logger.isDebugEnabled())
              logger.debug("Stream context metadata " + streamContexts[i]);
        }

        /* Set up the stream manager with the files that need to streamed */
        StreamManager.instance(target).addFilesToStream(streamContexts);
        /* Send the bootstrap initiate message */
        BootstrapInitiateMessage biMessage = new BootstrapInitiateMessage(streamContexts);
        Message message = BootstrapInitiateMessage.makeBootstrapInitiateMessage(biMessage);
        if (logger.isDebugEnabled())
          logger.debug("Sending a bootstrap initiate message to " + target + " ...");
        MessagingService.instance().sendOneWay(message, target);
        if (logger.isDebugEnabled())
          logger.debug("Waiting for transfer to " + target + " to complete");
        StreamManager.instance(target).waitForStreamCompletion();
        if (logger.isDebugEnabled())
          logger.debug("Done with transfer to " + target);
    }
}
