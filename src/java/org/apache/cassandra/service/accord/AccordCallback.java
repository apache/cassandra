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

package org.apache.cassandra.service.accord;

import accord.messages.Callback;
import accord.messages.Reply;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;

class AccordCallback<T extends Reply> implements RequestCallback<T>
{
    private final Callback<T> callback;

    public AccordCallback(Callback<T> callback)
    {
        this.callback = callback;
    }

    @Override
    public void onResponse(Message<T> msg)
    {
        callback.onSuccess(EndpointMapping.endpointToId(msg.from()), msg.payload);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        callback.onFailure(EndpointMapping.endpointToId(from), new RuntimeException(failureReason.toString()));
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }
}
