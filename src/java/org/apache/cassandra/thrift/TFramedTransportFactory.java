/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.thrift;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class TFramedTransportFactory implements ITransportFactory
{
    private static final String THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB = "cassandra.thrift.framed.size_mb";
    private int thriftFramedTransportSizeMb = 15; // 15Mb is the default for C* & Hadoop ConfigHelper

    public TTransport openTransport(String host, int port) throws TTransportException
    {
        TSocket socket = new TSocket(host, port);
        TTransport transport = new TFramedTransport(socket, thriftFramedTransportSizeMb * 1024 * 1024);
        transport.open();
        return transport;
    }

    public void setOptions(Map<String, String> options)
    {
        if (options.containsKey(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB))
            thriftFramedTransportSizeMb = Integer.parseInt(options.get(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB));
    }

    public Set<String> supportedOptions()
    {
        return Collections.singleton(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB);
    }
}
