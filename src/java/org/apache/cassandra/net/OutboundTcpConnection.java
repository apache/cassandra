package org.apache.cassandra.net;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;

public class OutboundTcpConnection extends Thread
{
    private static final Logger logger = Logger.getLogger(OutboundTcpConnection.class);

    private static final ByteBuffer CLOSE_SENTINEL = ByteBuffer.allocate(0);
    private static final int OPEN_RETRY_DELAY = 100; // ms between retries

    private final OutboundTcpConnectionPool pool;
    private final InetAddress endpoint;
    private final BlockingQueue<ByteBuffer> queue = new LinkedBlockingQueue<ByteBuffer>();
    private DataOutputStream output;
    private Socket socket;

    public OutboundTcpConnection(final OutboundTcpConnectionPool pool, final InetAddress remoteEp)
    {
        super("WRITE-" + remoteEp);
        this.pool = pool;
        this.endpoint = remoteEp;
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
        queue.clear();
        write(CLOSE_SENTINEL);
    }

    public void run()
    {
        while (true)
        {
            ByteBuffer bb = take();
            if (bb == CLOSE_SENTINEL)
            {
                disconnect();
                continue;
            }
            if (socket != null || connect())
                writeConnected(bb);
        }
    }

    private void writeConnected(ByteBuffer bb)
    {
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
            logger.info("error writing to " + endpoint);
            disconnect();
        }
    }

    private void disconnect()
    {
        if (socket != null)
        {
            try
            {
                socket.close();
            }
            catch (IOException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("exception closing connection to " + endpoint, e);
            }
            output = null;
            socket = null;
        }
    }

    private ByteBuffer take()
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
        return bb;
    }

    private boolean connect()
    {
        if (logger.isDebugEnabled())
            logger.debug("attempting to connect to " + endpoint);
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + DatabaseDescriptor.getRpcTimeout())
        {
            try
            {
                socket = new Socket(endpoint, DatabaseDescriptor.getStoragePort());
                socket.setTcpNoDelay(true);
                output = new DataOutputStream(socket.getOutputStream());
                return true;
            }
            catch (IOException e)
            {
                socket = null;
                if (logger.isTraceEnabled())
                    logger.trace("unable to connect to " + endpoint, e);
                try
                {
                    Thread.sleep(OPEN_RETRY_DELAY);
                }
                catch (InterruptedException e1)
                {
                    throw new AssertionError(e1);
                }
            }
        }
        return false;
    }
}
