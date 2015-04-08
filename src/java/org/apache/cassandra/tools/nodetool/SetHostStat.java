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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SetHostStat implements Iterable<HostStat>
{
    final List<HostStat> hostStats = new ArrayList<HostStat>();
    final boolean resolveIp;

    public SetHostStat(boolean resolveIp)
    {
        this.resolveIp = resolveIp;
    }

    public int size()
    {
        return hostStats.size();
    }

    @Override
    public Iterator<HostStat> iterator()
    {
        return hostStats.iterator();
    }

    public void add(String token, String host, Map<InetAddress, Float> ownerships) throws UnknownHostException
    {
        InetAddress endpoint = InetAddress.getByName(host);
        Float owns = ownerships.get(endpoint);
        hostStats.add(new HostStat(token, endpoint, resolveIp, owns));
    }
}