/**
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
package org.apache.cassandra.cli.transport;

import org.apache.cassandra.cli.CliMain;
import org.apache.cassandra.cli.CliSessionState;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

public class SSLTransportFactory extends TTransportFactory
{
    private static final int SOCKET_TIMEOUT = 0;

    public TTransport getTransport(TTransport trans)
    {
        final CliSessionState sessionState = CliMain.sessionState;
        try
        {
            TSSLTransportParameters params = new TSSLTransportParameters(sessionState.encOptions.protocol, sessionState.encOptions.cipher_suites);
            params.setTrustStore(sessionState.encOptions.truststore, sessionState.encOptions.truststore_password);
            trans = TSSLTransportFactory.getClientSocket(sessionState.hostName, sessionState.thriftPort, SOCKET_TIMEOUT, params);
            return new FramedTransportFactory().getTransport(trans);
        }
        catch (TTransportException e)
        {
            throw new RuntimeException("Failed to create a client SSL connection.", e);
        }
    }
}
