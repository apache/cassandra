package org.apache.cassandra.thrift;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

/**
 * Extends Thrift's TServerSocket to allow customization of various desirable
 * TCP properties.
 */
public class TCustomServerSocket extends TServerSocket
{

    private static final Logger logger = LoggerFactory.getLogger(TCustomServerSocket.class);

    private final boolean keepAlive;
    private final Integer sendBufferSize;
    private final Integer recvBufferSize;

    /**
     * Allows fine-tuning of the server socket including keep-alive, reuse of addresses, send and receive buffer sizes.
     * @param bindAddr
     * @param keepAlive
     * @param sendBufferSize
     * @param recvBufferSize
     * @throws TTransportException
     */
    public TCustomServerSocket(InetSocketAddress bindAddr, boolean keepAlive, Integer sendBufferSize, Integer recvBufferSize)
    throws TTransportException
    {
        super(bindAddr);
        this.keepAlive = keepAlive;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
    }

    @Override
    protected TSocket acceptImpl() throws TTransportException
    {
        TSocket tsocket = super.acceptImpl();
        Socket socket = tsocket.getSocket();

        try
        {
            socket.setKeepAlive(this.keepAlive);
        }
        catch (SocketException se)
        {
            logger.warn("Failed to set keep-alive on Thrift socket.", se);
        }

        if (this.sendBufferSize != null)
        {
            try
            {
                socket.setSendBufferSize(this.sendBufferSize.intValue());
            }
            catch (SocketException se)
            {
                logger.warn("Failed to set send buffer size on Thrift socket.", se);
            }
        }

        if (this.recvBufferSize != null)
        {
            try
            {
                socket.setReceiveBufferSize(this.recvBufferSize.intValue());
            }
            catch (SocketException se)
            {
                logger.warn("Failed to set receive buffer size on Thrift socket.", se);
            }
        }

        return tsocket;
    }
}
