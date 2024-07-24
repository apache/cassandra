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

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.rmi.server.RMIClientSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.cassandra.config.EncryptionOptions;

public class JmxSslRMIClientSocketFactory implements RMIClientSocketFactory, Serializable
{
    private EncryptionOptions jmxEncryptionOptions;
    public JmxSslRMIClientSocketFactory(EncryptionOptions jmxEncryptionOptions)
    {
        this.jmxEncryptionOptions = jmxEncryptionOptions;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException
    {
        // Retrieve the SSLSocketFactory
        //
        final SocketFactory sslSocketFactory = jmxEncryptionOptions.sslContextFactoryInstance.createJSSESslContext(jmxEncryptionOptions.getClientAuth()).getSocketFactory();
        // Create the SSLSocket
        final SSLSocket sslSocket = (SSLSocket)
                                    sslSocketFactory.createSocket(host, port);
        // Set the SSLSocket Enabled Cipher Suites
        final String[] enabledCipherSuites = jmxEncryptionOptions.cipherSuitesArray();
        if (enabledCipherSuites != null) {
            try {
                sslSocket.setEnabledCipherSuites(enabledCipherSuites);
            } catch (IllegalArgumentException e) {
                throw new IOException(e.getMessage());
            }
        }
        // Set the SSLSocket Enabled Protocols
        final String[] enabledProtocols = jmxEncryptionOptions.acceptedProtocolsArray();
        if (enabledProtocols != null) {
            try {
                sslSocket.setEnabledProtocols(enabledProtocols);
            } catch (IllegalArgumentException e) {
                throw new IOException(e.getMessage());
            }
        }
        return sslSocket;
    }

    /**
     * <p>Indicates whether some other object is "equal to" this one.</p>
     *
     * <p>Because all instances of this class are functionally equivalent
     * (they all use the default
     * <code>SSLSocketFactory</code>), this method simply returns
     * <code>this.getClass().equals(obj.getClass())</code>.</p>
     *
     * <p>A subclass should override this method (as well
     * as {@link #hashCode()}) if its instances are not all
     * functionally equivalent.</p>
     */
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        return this.getClass().equals(obj.getClass());
    }

    /**
     * <p>Returns a hash code value for this
     * <code>JmxSslRMIClientSocketFactory</code>.</p>
     *
     * @return a hash code value for this
     * <code>JmxSslRMIClientSocketFactory</code>.
     */
    public int hashCode() {
        return this.getClass().hashCode();
    }

    private static SocketFactory defaultSocketFactory = null;

    private static synchronized SocketFactory getDefaultClientSocketFactory() {
        if (defaultSocketFactory == null)
            defaultSocketFactory = SSLSocketFactory.getDefault();
        return defaultSocketFactory;
    }
}
