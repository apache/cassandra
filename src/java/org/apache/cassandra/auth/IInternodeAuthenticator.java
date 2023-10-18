/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.security.cert.Certificate;

import org.apache.cassandra.exceptions.ConfigurationException;

public interface IInternodeAuthenticator
{
    /**
     * Decides whether or not a peer is allowed to connect to this node.
     * If this method returns false, the socket will be immediately closed.
     *
     * @param remoteAddress ip address of the connecting node.
     * @param remotePort port of the connecting node.
     * @return true if the connection should be accepted, false otherwise.
     * @deprecated See CASSANDRA-17661
     */
    @Deprecated(since = "5.0")
    default boolean authenticate(InetAddress remoteAddress, int remotePort)
    {
        return false;
    }

    /**
     * Decides whether a peer is allowed to connect to this node.
     * If this method returns false, the socket will be immediately closed.
     * <p>
     * Default implementation calls authenticate method by IP and port method
     * <p>
     * 1. If it is IP based authentication ignore the certificates & connectionType parameters in the implementation
     * of this method.
     * 2. For certificate based authentication like mTLS, server's identity for outbound connections is verified by the
     * trusted root certificates in the outbound_keystore. In such cases this method may be overridden to return true
     * when certificateType is OUTBOUND, as the authentication of the server happens during SSL Handshake.
     *
     * @param remoteAddress  ip address of the connecting node.
     * @param remotePort     port of the connecting node.
     * @param certificates   peer certificates
     * @param connectionType If the connection is inbound/outbound connection.
     * @return true if the connection should be accepted, false otherwise.
     */
    default boolean authenticate(InetAddress remoteAddress, int remotePort,
                                 Certificate[] certificates, InternodeConnectionDirection connectionType)
    {
        return authenticate(remoteAddress, remotePort);
    }

    /**
     * Validates configuration of IInternodeAuthenticator implementation (if configurable).
     *
     * @throws ConfigurationException when there is a configuration error.
     */
    void validateConfiguration() throws ConfigurationException;

    /**
     * Setup is called once upon system startup to initialize the IAuthenticator.
     *
     * For example, use this method to do any required initialization of the class.
     */
    default void setupInternode()
    {

    }

    /**
     * Enum that represents connection type of internode connection.
     *
     * INBOUND - called after connection established, with certificate available if present.
     * OUTBOUND - called after connection established, with certificate available if present.
     * OUTBOUND_PRECONNECT - called before initiating a connection, without certificate available.
     * The outbound connection will be authenticated with the certificate once a redirected connection is established.
     * This is an extra check that can be used to detect misconfiguration before reconnection, or ignored by returning true.
     */
    enum InternodeConnectionDirection
    {
        INBOUND,
        OUTBOUND,
        OUTBOUND_PRECONNECT
    }
}
