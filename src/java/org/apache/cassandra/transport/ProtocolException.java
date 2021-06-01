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

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.TransportException;

/**
 * Exceptions thrown when a client didn't respect the protocol.
 */
public class ProtocolException extends RuntimeException implements TransportException
{
    private final Integer attemptedLowProtocolVersion;

    public ProtocolException(String msg)
    {
        this(msg, null);
    }

    public ProtocolException(String msg, Integer attemptedLowProtocolVersion)
    {
        super(msg);
        this.attemptedLowProtocolVersion = attemptedLowProtocolVersion;
    }

    public ExceptionCode code()
    {
        return ExceptionCode.PROTOCOL_ERROR;
    }

    /**
     * If the ProtocolException is due to a connection being made with a protocol version that is lower
     * than Server.MIN_SUPPORTED_VERSION, this will return that unsupported protocol version.  Otherwise,
     * null is returned.
     */
    public Integer getAttemptedLowProtocolVersion()
    {
        return attemptedLowProtocolVersion;
    }

    public boolean isSilent()
    {
        return false;
    }

    public static ProtocolException toSilentException(ProtocolException e)
    {
        return new Silent(e);
    }

    private static class Silent extends ProtocolException
    {
        public Silent(ProtocolException cause)
        {
            super(cause.getMessage(), cause.attemptedLowProtocolVersion);
        }

        @Override
        public boolean isSilent()
        {
            return true;
        }
    }
}
