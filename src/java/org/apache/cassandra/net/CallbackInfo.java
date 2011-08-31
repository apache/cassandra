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

package org.apache.cassandra.net;

import java.net.InetAddress;

import org.apache.cassandra.service.StorageProxy;

/**
 * Encapsulates the callback information.
 * The ability to set the message is useful in cases for when a hint needs 
 * to be written due to a timeout in the response from a replica.
 */
class CallbackInfo
{
    protected final InetAddress target;
    protected final IMessageCallback callback;
    protected final Message message;

    public CallbackInfo(InetAddress target, IMessageCallback callback)
    {
        this.target = target;
        this.callback = callback;
        this.message = null;
    }

    public CallbackInfo(InetAddress target, IMessageCallback callback, Message message)
    {
        this.target = target;
        this.callback = callback;
        this.message = message;
    }

    /**
     * @return TRUE if a hint should be written for this target and if the CL was achieved. FALSE otherwise.
     *
     * NOTE:
     * Assumes it is only called after the write of "message" to "target" has timed out.
     */
    public boolean shouldHint()
    {
        return message != null && StorageProxy.shouldHint(target);
    }
}
