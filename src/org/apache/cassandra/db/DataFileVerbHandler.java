package org.apache.cassandra.db;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;


public class DataFileVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger( DataFileVerbHandler.class );
    
    public void doVerb(Message message)
    {        
        Object[] body = message.getMessageBody();
        byte[] bytes = (byte[])body[0];
        String table = new String(bytes);
        logger_.info("**** Received a request from " + message.getFrom());
        
        List<String> allFiles = Table.open(table).getAllSSTablesOnDisk();        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try
        {
            dos.writeInt(allFiles.size());
            for ( String file : allFiles )
            {
                dos.writeUTF(file);
            }
            Message response = message.getReply( StorageService.getLocalStorageEndPoint(), new Object[]{bos.toByteArray()});
            MessagingService.getMessagingInstance().sendOneWay(response, message.getFrom());
        }
        catch ( IOException ex )
        {
            logger_.warn(LogUtil.throwableToString(ex));
        }
    }
}
