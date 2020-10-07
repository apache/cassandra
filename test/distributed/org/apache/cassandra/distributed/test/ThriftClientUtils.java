/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
