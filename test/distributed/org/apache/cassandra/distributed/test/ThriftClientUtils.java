package org.apache.cassandra.distributed.test;

import java.net.InetSocketAddress;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TFramedTransportFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

public final class ThriftClientUtils
{
    private ThriftClientUtils()
    {

    }

    public static void thriftClient(IInstance instance, ThriftConsumer fn) throws TException
    {
        //TODO dtest APIs only expose native address, doesn't expose all addresses we listen to, so assume the default thrift port
        thriftClient(new InetSocketAddress(instance.broadcastAddress().getAddress(), 9160), fn);
    }

    public static void thriftClient(InetSocketAddress address, ThriftConsumer fn) throws TException
    {
        try (TTransport transport = new TFramedTransportFactory().openTransport(address.getAddress().getHostAddress(), address.getPort()))
        {
            Cassandra.Client client = new Cassandra.Client(new TBinaryProtocol(transport));
            fn.accept(client);
        }
    }

    public interface ThriftConsumer
    {
        void accept(Cassandra.Client client) throws TException;
    }
}
