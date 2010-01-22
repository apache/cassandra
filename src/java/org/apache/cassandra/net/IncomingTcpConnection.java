package org.apache.cassandra.net;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import org.apache.cassandra.net.io.IncomingStreamReader;
import org.apache.cassandra.utils.FBUtilities;

public class IncomingTcpConnection extends Thread
{
    private static Logger logger = Logger.getLogger(IncomingTcpConnection.class);

    private final DataInputStream input;
    private Socket socket;

    public IncomingTcpConnection(Socket socket)
    {
        this.socket = socket;
        try
        {
            input = new DataInputStream(socket.getInputStream());
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    @Override
    public void run()
    {
        while (true)
        {
            try
            {
                MessagingService.validateMagic(input.readInt());
                int header = input.readInt();
                int type = MessagingService.getBits(header, 1, 2);
                boolean isStream = MessagingService.getBits(header, 3, 1) == 1;
                int version = MessagingService.getBits(header, 15, 8);

                if (isStream)
                {
                    new IncomingStreamReader(socket.getChannel()).read();
                }
                else
                {
                    int size = input.readInt();
                    byte[] contentBytes = new byte[size];
                    input.readFully(contentBytes);
                    MessagingService.getDeserializationExecutor().submit(new MessageDeserializationTask(new ByteArrayInputStream(contentBytes)));
                }
            }
            catch (EOFException e)
            {
                if (logger.isTraceEnabled())
                    logger.trace("eof reading from socket; closing", e);
                break;
            }
            catch (IOException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("error reading from socket; closing", e);
                break;
            }
        }
    }
}
