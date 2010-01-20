package org.apache.cassandra.net;

import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

public class OutboundTcpConnection
{
    private static Logger logger = Logger.getLogger(OutboundTcpConnection.class);

    public BlockingQueue<ByteBuffer> queue = new LinkedBlockingQueue<ByteBuffer>();
    public DataOutputStream output;
    public Socket socket;

    // TODO localEp is ignored, get rid of it
    public OutboundTcpConnection(final OutboundTcpConnectionPool pool, InetAddress localEp, final InetAddress remoteEp)
    {
        try
        {
            socket = new Socket(remoteEp, DatabaseDescriptor.getStoragePort());
            socket.setTcpNoDelay(true);
            output = new DataOutputStream(socket.getOutputStream());
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        new Thread(new Runnable()
        {
            public void run()
            {
                while (socket != null)
                {
                    ByteBuffer bb;
                    try
                    {
                        bb = queue.take();
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }
                    try
                    {
                        output.write(bb.array(), 0, bb.limit());
                        if (queue.peek() == null)
                        {
                            output.flush();
                        }
                    }
                    catch (IOException e)
                    {
                        logger.info("error writing to " + remoteEp);
                        pool.reset();
                        break;
                    }
                }
            }
        }, "WRITE-" + remoteEp).start();
    }

    public void write(ByteBuffer buffer)
    {
        try
        {
            queue.put(buffer);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    public void closeSocket()
    {
        try
        {
            socket.close();
        }
        catch (IOException e)
        {
            if (logger.isDebugEnabled())
                logger.debug("error closing socket", e);
        }
        socket = null;
    }
}
