package org.apache.cassandra.thrift;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import org.apache.cassandra.service.SocketSessionManagementService;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCustomNonblockingServerSocket extends TNonblockingServerSocket
{
    private static final Logger logger = LoggerFactory.getLogger(TCustomNonblockingServerSocket.class);
    private final boolean keepAlive;
    private final Integer sendBufferSize;
    private final Integer recvBufferSize;

    public TCustomNonblockingServerSocket(InetSocketAddress bindAddr, boolean keepAlive, Integer sendBufferSize, Integer recvBufferSize) throws TTransportException
    {
        super(bindAddr);
        this.keepAlive = keepAlive;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
    }

    @Override
    protected TNonblockingSocket acceptImpl() throws TTransportException
    {
        TNonblockingSocket tsocket = super.acceptImpl();
        if (tsocket == null || tsocket.getSocketChannel() == null)
            return tsocket;
        Socket socket = tsocket.getSocketChannel().socket();
        // clean up the old information.
        SocketSessionManagementService.instance.remove(socket.getRemoteSocketAddress());
        try
        {
            socket.setKeepAlive(this.keepAlive);
        } catch (SocketException se)
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