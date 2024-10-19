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

package org.apache.cassandra.utils;

import javax.net.ssl.SSLException;
import javax.rmi.ssl.SslRMIServerSocketFactory;

import org.apache.cassandra.config.EncryptionOptions;

/**
 * This class extends {@code SslRMIServerSocketFactory} to use {@code jmx_encryption_options}, configured via
 * cassandra.yaml, for creating the JMX server socket.
 *
 * @see javax.rmi.ssl.SslRMIServerSocketFactory
 */
public class JMXSslRMIServerSocketFactory extends SslRMIServerSocketFactory
{
    public JMXSslRMIServerSocketFactory(EncryptionOptions jmxEncryptionOptions) throws SSLException
    {
        super(jmxEncryptionOptions.sslContextFactoryInstance.createJSSESslContext(jmxEncryptionOptions.getClientAuth()),
              jmxEncryptionOptions.cipherSuitesArray(),
              jmxEncryptionOptions.acceptedProtocolsArray(),
              jmxEncryptionOptions.getClientAuth() == EncryptionOptions.ClientAuth.REQUIRED);
    }
}
