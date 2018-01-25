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
package org.apache.cassandra.net;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Encapsulates the callback information.
 * The ability to set the message is useful in cases for when a hint needs 
 * to be written due to a timeout in the response from a replica.
 */
public class CallbackInfo
{
    protected final InetAddressAndPort target;
    protected final IAsyncCallback callback;
    protected final IVersionedSerializer<?> serializer;
    private final boolean failureCallback;

    /**
     * Create CallbackInfo without sent message
     *
     * @param target target to send message
     * @param callback
     * @param serializer serializer to deserialize response message
     * @param failureCallback True when we have a callback to handle failures
     */
    public CallbackInfo(InetAddressAndPort target, IAsyncCallback callback, IVersionedSerializer<?> serializer, boolean failureCallback)
    {
        this.target = target;
        this.callback = callback;
        this.serializer = serializer;
        this.failureCallback = failureCallback;
    }

    public boolean shouldHint()
    {
        return false;
    }

    public boolean isFailureCallback()
    {
        return failureCallback;
    }

    public String toString()
    {
        return "CallbackInfo(" +
               "target=" + target +
               ", callback=" + callback +
               ", serializer=" + serializer +
               ", failureCallback=" + failureCallback +
               ')';
    }
}
