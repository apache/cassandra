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
package org.apache.cassandra.thrift;

import java.net.InetSocketAddress;

import java.nio.channels.SelectionKey;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

public class CustomTNonBlockingServer extends TNonblockingServer
{
    public CustomTNonBlockingServer(Args args)
    {
        super(args);
    }

    @Override
    @SuppressWarnings("resource")
    protected boolean requestInvoke(FrameBuffer frameBuffer)
    {
        TNonblockingSocket socket = (TNonblockingSocket)((CustomFrameBuffer)frameBuffer).getTransport();
        ThriftSessionManager.instance.setCurrentSocket(socket.getSocketChannel().socket().getRemoteSocketAddress());
        frameBuffer.invoke();
        return true;
    }

    public static class Factory implements TServerFactory
    {
        @SuppressWarnings("resource")
        public TServer buildTServer(Args args)
        {
            if (DatabaseDescriptor.getClientEncryptionOptions().enabled)
                throw new RuntimeException("Client SSL is not supported for non-blocking sockets. Please remove client ssl from the configuration.");

            final InetSocketAddress addr = args.addr;
            TNonblockingServerTransport serverTransport;
            try
            {
                serverTransport = new TCustomNonblockingServerSocket(addr, args.keepAlive, args.sendBufferSize, args.recvBufferSize);
            }
            catch (TTransportException e)
            {
                throw new RuntimeException(String.format("Unable to create thrift socket to %s:%s", addr.getAddress(), addr.getPort()), e);
            }

            // This is single threaded hence the invocation will be all
            // in one thread.
            TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport).inputTransportFactory(args.inTransportFactory)
                                                                                             .outputTransportFactory(args.outTransportFactory)
                                                                                             .inputProtocolFactory(args.tProtocolFactory)
                                                                                             .outputProtocolFactory(args.tProtocolFactory)
                                                                                             .processor(args.processor);
            return new CustomTNonBlockingServer(serverArgs);
        }
    }

    public class CustomFrameBuffer extends FrameBuffer
    {
        public CustomFrameBuffer(final TNonblockingTransport trans,
          final SelectionKey selectionKey,
          final AbstractSelectThread selectThread) {
			super(trans, selectionKey, selectThread);
        }

        public TNonblockingTransport getTransport() {
            return this.trans_;
        }
    }
}
