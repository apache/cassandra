package org.apache.cassandra.db;

import java.io.IOException;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;


public class TouchVerbHandler implements IVerbHandler
{
    private static class ReadContext
    {
        protected DataInputBuffer bufIn_ = new DataInputBuffer();
    }

	
    private static Logger logger_ = Logger.getLogger( ReadVerbHandler.class );
    /* We use this so that we can reuse the same row mutation context for the mutation. */
    private static ThreadLocal<ReadContext> tls_ = new InheritableThreadLocal<ReadContext>();

    public void doVerb(Message message)
    {
        byte[] body = (byte[])message.getMessageBody()[0];
        /* Obtain a Read Context from TLS */
        ReadContext readCtx = tls_.get();
        if ( readCtx == null )
        {
            readCtx = new ReadContext();
            tls_.set(readCtx);
        }
        readCtx.bufIn_.reset(body, body.length);

        try
        {
            TouchMessage touchMessage = TouchMessage.serializer().deserialize(readCtx.bufIn_);
            Table table = Table.open(touchMessage.table());
   			table.touch(touchMessage.key(), touchMessage.isData());
        }
        catch ( IOException ex)
        {
            logger_.info( LogUtil.throwableToString(ex) );
        }
    }
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		// TODO Auto-generated method stub

	}

}
