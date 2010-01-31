package org.apache.cassandra.streaming;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.SSTableWriter;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.IStreamComplete;
import org.apache.cassandra.streaming.StreamInManager;
import org.apache.cassandra.service.StorageService;

/**
 * This is the callback handler that is invoked when we have
 * completely received a single file from a remote host.
 *
 * TODO if we move this into CFS we could make addSSTables private, improving encapsulation.
*/
class StreamCompletionHandler implements IStreamComplete
{
    private static Logger logger = Logger.getLogger(StreamCompletionHandler.class);

    public void onStreamCompletion(InetAddress host, InitiatedFile initiatedFile, StreamInManager.StreamStatus streamStatus) throws IOException
    {
        /* Parse the stream context and the file to the list of SSTables in the associated Column Family Store. */
        if (initiatedFile.getTargetFile().contains("-Data.db"))
        {
            String tableName = initiatedFile.getTable();
            File file = new File( initiatedFile.getTargetFile() );
            String fileName = file.getName();
            String [] temp = fileName.split("-");

            //Open the file to see if all parts are now here
            try
            {
                SSTableReader sstable = SSTableWriter.renameAndOpen(initiatedFile.getTargetFile());
                //TODO add a sanity check that this sstable has all its parts and is ok
                Table.open(tableName).getColumnFamilyStore(temp[0]).addSSTable(sstable);
                logger.info("Streaming added " + sstable.getFilename());
            }
            catch (IOException e)
            {
                throw new RuntimeException("Not able to add streamed file " + initiatedFile.getTargetFile(), e);
            }
        }

        if (logger.isDebugEnabled())
          logger.debug("Sending a streaming finished message with " + streamStatus + " to " + host);
        /* Send a StreamStatusMessage object which may require the source node to re-stream certain files. */
        StreamInManager.StreamStatusMessage streamStatusMessage = new StreamInManager.StreamStatusMessage(streamStatus);
        Message message = StreamInManager.StreamStatusMessage.makeStreamStatusMessage(streamStatusMessage);
        MessagingService.instance.sendOneWay(message, host);

        /* If we're done with everything for this host, remove from bootstrap sources */
        if (StreamInManager.isDone(host) && StorageService.instance.isBootstrapMode())
        {
            StorageService.instance.removeBootstrapSource(host, initiatedFile.getTable());
        }
    }
}
