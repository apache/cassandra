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
package org.apache.cassandra.transport;

import org.apache.cassandra.service.ClientState;

public class ServerConnection extends Connection
{
    public static final Factory FACTORY = new Factory()
    {
        public Connection newConnection(Connection.Tracker tracker)
        {
            return new ServerConnection(tracker);
        }
    };

    private enum State { UNINITIALIZED, AUTHENTICATION, READY; }

    private final ClientState clientState;
    private volatile State state;

    public ServerConnection(Connection.Tracker tracker)
    {
        super(tracker);
        this.clientState = new ClientState();
        this.state = State.UNINITIALIZED;
    }

    public ClientState clientState()
    {
        return clientState;
    }

    public void validateNewMessage(Message.Type type)
    {
        switch (state)
        {
            case UNINITIALIZED:
                if (type != Message.Type.STARTUP && type != Message.Type.OPTIONS)
                    throw new ProtocolException(String.format("Unexpected message %s, expecting STARTUP or OPTIONS", type));
                break;
            case AUTHENTICATION:
                if (type != Message.Type.CREDENTIALS)
                    throw new ProtocolException(String.format("Unexpected message %s, needs authentication through CREDENTIALS message", type));
                break;
            case READY:
                if (type == Message.Type.STARTUP)
                    throw new ProtocolException("Unexpected message STARTUP, the connection is already initialized");
                break;
            default:
                throw new AssertionError();
        }
    }

    public void applyStateTransition(Message.Type requestType, Message.Type responseType)
    {
        switch (state)
        {
            case UNINITIALIZED:
                if (requestType == Message.Type.STARTUP)
                {
                    if (responseType == Message.Type.AUTHENTICATE)
                        state = State.AUTHENTICATION;
                    else if (responseType == Message.Type.READY)
                        state = State.READY;
                }
                break;
            case AUTHENTICATION:
                assert requestType == Message.Type.CREDENTIALS;
                if (responseType == Message.Type.READY)
                    state = State.READY;
            case READY:
                break;
            default:
                throw new AssertionError();
        }
    }
}
