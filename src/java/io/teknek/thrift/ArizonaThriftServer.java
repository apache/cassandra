
package io.teknek.thrift;

import io.teknek.ArizonaServer;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.thrift.TServerCustomFactory;
import org.apache.cassandra.thrift.TServerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class ArizonaThriftServer implements CassandraDaemon.Server
{
    private static Logger logger = LoggerFactory.getLogger(ArizonaThriftServer.class);
    protected final static String SYNC = "sync";
    protected final static String ASYNC = "async";
    protected final static String HSHA = "hsha";

    protected final InetAddress address;
    protected final int port;
    protected final int backlog;
    private volatile ArizonaServerThread server;

    public ArizonaThriftServer(InetAddress address, int port, int backlog)
    {
        this.address = address;
        this.port = port;
        this.backlog = backlog;
    }

    public void start()
    {
        if (server == null)
        {
            ArizonaServer iface = getArizonaServer();
            server = new ArizonaServerThread(address, port, backlog, iface, getProcessor(iface), getTransportFactory());
            server.start();
        }
    }

    public void stop()
    {
        if (server != null)
        {
            server.stopServer();
            try
            {
                server.join();
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting thrift server to stop", e);
            }
            server = null;
        }
    }

    public boolean isRunning()
    {
        return server != null;
    }

    /*
     * These methods are intended to be overridden to provide custom implementations.
     */
    protected ArizonaServer getArizonaServer()
    {
        return new ArizonaServer();
    }

    protected TProcessor getProcessor(ArizonaServer server)
    {
        return new io.teknek.arizona.Arizona.Processor<io.teknek.arizona.Arizona.Iface>(server);
    }

    protected TTransportFactory getTransportFactory()
    {
        int tFramedTransportSize = DatabaseDescriptor.getThriftFramedTransportSize();
        return new TFramedTransport.Factory(tFramedTransportSize);
    }

    /**
     * Simple class to run the thrift connection accepting code in separate
     * thread of control.
     */
    private static class ArizonaServerThread extends Thread
    {
        private final TServer serverEngine;

        public ArizonaServerThread(InetAddress listenAddr,
                                  int listenPort,
                                  int listenBacklog,
                                  ArizonaServer server,
                                  TProcessor processor,
                                  TTransportFactory transportFactory)
        {
            // now we start listening for clients
            logger.info(String.format("Binding thrift service to %s:%s", listenAddr, listenPort));

            TServerFactory.Args args = new TServerFactory.Args();
            args.tProtocolFactory = new TBinaryProtocol.Factory(true, true);
            args.addr = new InetSocketAddress(listenAddr, listenPort);
            args.listenBacklog = listenBacklog;
            args.processor = processor;
            args.keepAlive = DatabaseDescriptor.getRpcKeepAlive();
            args.sendBufferSize = DatabaseDescriptor.getRpcSendBufferSize();
            args.recvBufferSize = DatabaseDescriptor.getRpcRecvBufferSize();
            args.inTransportFactory = transportFactory;
            args.outTransportFactory = transportFactory;
            serverEngine = new TServerCustomFactory(DatabaseDescriptor.getRpcServerType()).buildTServer(args);
        }

        public void run()
        {
            logger.info("Listening for thrift clients...");
            serverEngine.serve();
        }

        public void stopServer()
        {
            logger.info("Stop listening to thrift clients");
            serverEngine.stop();
        }
    }
}
