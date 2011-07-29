package org.apache.cassandra.thrift;

import org.apache.cassandra.service.SocketSessionManagementService;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TNonblockingSocket;

public class CustomTNonBlockingServer extends TNonblockingServer
{
    public CustomTNonBlockingServer(Args args)
    {
        super(args);
    }

    @Override
    protected boolean requestInvoke(FrameBuffer frameBuffer)
    {
        TNonblockingSocket socket = (TNonblockingSocket) frameBuffer.trans_;
        SocketSessionManagementService.remoteSocket.set(socket.getSocketChannel().socket().getRemoteSocketAddress());
        frameBuffer.invoke();
        return true;
    }
}
