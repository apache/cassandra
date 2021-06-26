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

package org.apache.cassandra.tools.nodetool;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.locator.InetAddressAndPort;

public class SetHostStatWithPort implements Iterable<HostStatWithPort>
{
    final List<HostStatWithPort> hostStats = new ArrayList<>();
    final boolean resolveIp;

    public SetHostStatWithPort(boolean resolveIp)
    {
        this.resolveIp = resolveIp;
    }

    public int size()
    {
        return hostStats.size();
    }

    @Override
    public Iterator<HostStatWithPort> iterator()
    {
        return hostStats.iterator();
    }

    public void add(String token, String host, Map<String, Float> ownerships) throws UnknownHostException
    {
        InetAddressAndPort endpoint = InetAddressAndPort.getByName(host);
        Float owns = ownerships.get(endpoint.getHostAddressAndPort());
        hostStats.add(new HostStatWithPort(token, endpoint, resolveIp, owns));
    }
}
