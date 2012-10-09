/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.thrift;

import java.net.InetSocketAddress;

import javax.net.ssl.SSLContext;

import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportFactory;

public interface TServerFactory
{
    TServer buildTServer(Args args);

    public static class Args
    {
        public InetSocketAddress addr;
        public CassandraServer cassandraServer;
        public Cassandra.Processor processor;
        public TProtocolFactory tProtocolFactory;
        public TTransportFactory inTransportFactory;
        public TTransportFactory outTransportFactory;
        public Integer sendBufferSize;
        public Integer recvBufferSize;
        public boolean keepAlive;
        public SSLContext ctx;
        public String[] cipherSuits;
    }
}