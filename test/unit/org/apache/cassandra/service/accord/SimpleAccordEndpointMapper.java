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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import accord.local.Node;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.ByteBufferUtil;

public enum SimpleAccordEndpointMapper implements AccordEndpointMapper
{
    INSTANCE;

    @Override
    public Node.Id mappedId(InetAddressAndPort endpoint)
    {
        if (endpoint.addressBytes.length != 4)
            throw new IllegalArgumentException("Only IPV4 is allowed: given " + endpoint.toString(true));
        return new Node.Id(ByteBuffer.wrap(endpoint.addressBytes).getInt());
    }

    @Override
    public InetAddressAndPort mappedEndpoint(Node.Id id)
    {
        byte[] array = ByteBufferUtil.bytes(id.id).array();
        try
        {
            return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(array), 1);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError("Unable to convert " + id + " to an IPV4 address", e);
        }
    }
}
