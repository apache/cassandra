package org.apache.cassandra.streaming;

import java.io.*;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.streaming.StreamInitiateMessage;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamInManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class StreamInitiateVerbHandler implements IVerbHandler
{
    private static Logger logger = Logger.getLogger(StreamInitiateVerbHandler.class);

    /*
     * Here we handle the StreamInitiateMessage. Here we get the
     * array of StreamContexts. We get file names for the column
     * families associated with the files and replace them with the
     * file names as obtained from the column family store on the
     * receiving end.
    */
    public void doVerb(Message message)
    {
        byte[] body = message.getMessageBody();
        ByteArrayInputStream bufIn = new ByteArrayInputStream(body);
        if (logger.isDebugEnabled())
            logger.debug(String.format("StreamInitiateVerbeHandler.doVerb %s %s %s", message.getVerb(), message.getMessageId(), message.getMessageType()));

        try
        {
            StreamInitiateMessage biMsg = StreamInitiateMessage.serializer().deserialize(new DataInputStream(bufIn));
            PendingFile[] pendingFiles = biMsg.getStreamContext();

            if (pendingFiles.length == 0)
            {
                if (logger.isDebugEnabled())
                    logger.debug("no data needed from " + message.getFrom());
                if (StorageService.instance.isBootstrapMode())
                    StorageService.instance.removeBootstrapSource(message.getFrom(), new String(message.getHeader(StreamOut.TABLE_NAME)));
                return;
            }

            Map<String, String> fileNames = getNewNames(pendingFiles);
            Map<String, String> pathNames = new HashMap<String, String>();
            for (String ssName : fileNames.keySet())
                pathNames.put(ssName, DatabaseDescriptor.getNextAvailableDataLocation());
            /*
             * For each of stream context's in the incoming message
             * generate the new file names and store the new file names
             * in the StreamContextManager.
            */
            for (PendingFile pendingFile : pendingFiles)
            {
                CompletedFileStatus streamStatus = new CompletedFileStatus(pendingFile.getTargetFile(), pendingFile.getExpectedBytes() );
                String file = getNewFileNameFromOldContextAndNames(fileNames, pathNames, pendingFile);

                if (logger.isDebugEnabled())
                  logger.debug("Received Data from  : " + message.getFrom() + " " + pendingFile.getTargetFile() + " " + file);
                pendingFile.setTargetFile(file);
                addStreamContext(message.getFrom(), pendingFile, streamStatus);
            }

            StreamInManager.registerStreamCompletionHandler(message.getFrom(), new StreamCompletionHandler());
            if (logger.isDebugEnabled())
              logger.debug("Sending a stream initiate done message ...");
            Message doneMessage = new Message(FBUtilities.getLocalAddress(), "", StorageService.Verb.STREAM_INITIATE_DONE, new byte[0] );
            MessagingService.instance.sendOneWay(doneMessage, message.getFrom());
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }

    public String getNewFileNameFromOldContextAndNames(Map<String, String> fileNames,
                                                       Map<String, String> pathNames,
                                                       PendingFile pendingFile)
    {
        File sourceFile = new File( pendingFile.getTargetFile() );
        String[] piece = FBUtilities.strip(sourceFile.getName(), "-");
        String cfName = piece[0];
        String ssTableNum = piece[1];
        String typeOfFile = piece[2];

        String newFileNameExpanded = fileNames.get(pendingFile.getTable() + "-" + cfName + "-" + ssTableNum);
        String path = pathNames.get(pendingFile.getTable() + "-" + cfName + "-" + ssTableNum);
        //Drop type (Data.db) from new FileName
        String newFileName = newFileNameExpanded.replace("Data.db", typeOfFile);
        return path + File.separator + pendingFile.getTable() + File.separator + newFileName;
    }

    // todo: this method needs to be private, or package at the very least for easy unit testing.
    public Map<String, String> getNewNames(PendingFile[] pendingFiles) throws IOException
    {
        /*
         * Mapping for each file with unique CF-i ---> new file name. For eg.
         * for a file with name <CF>-<i>-Data.db there is a corresponding
         * <CF>-<i>-Index.db. We maintain a mapping from <CF>-<i> to a newly
         * generated file name.
        */
        Map<String, String> fileNames = new HashMap<String, String>();
        /* Get the distinct entries from StreamContexts i.e have one entry per Data/Index/Filter file set */
        Set<String> distinctEntries = new HashSet<String>();
        for ( PendingFile pendingFile : pendingFiles)
        {
            String[] pieces = FBUtilities.strip(new File(pendingFile.getTargetFile()).getName(), "-");
            distinctEntries.add(pendingFile.getTable() + "-" + pieces[0] + "-" + pieces[1] );
        }

        /* Generate unique file names per entry */
        for ( String distinctEntry : distinctEntries )
        {
            String tableName;
            String[] pieces = FBUtilities.strip(distinctEntry, "-");
            tableName = pieces[0];
            Table table = Table.open( tableName );

            ColumnFamilyStore cfStore = table.getColumnFamilyStore(pieces[1]);
            if (logger.isDebugEnabled())
              logger.debug("Generating file name for " + distinctEntry + " ...");
            fileNames.put(distinctEntry, cfStore.getTempSSTableFileName());
        }

        return fileNames;
    }

    private void addStreamContext(InetAddress host, PendingFile pendingFile, CompletedFileStatus streamStatus)
    {
        if (logger.isDebugEnabled())
          logger.debug("Adding stream context " + pendingFile + " for " + host + " ...");
        StreamInManager.addStreamContext(host, pendingFile, streamStatus);
    }
}
