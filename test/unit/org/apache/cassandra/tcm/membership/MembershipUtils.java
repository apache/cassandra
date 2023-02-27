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

package org.apache.cassandra.tcm.membership;

import java.net.UnknownHostException;
import java.util.Random;

import org.apache.cassandra.locator.InetAddressAndPort;

public class MembershipUtils
{
    public static InetAddressAndPort randomEndpoint(Random random)
    {
        return endpoint(random.nextInt(254) + 1);
    }

    public static InetAddressAndPort endpoint(int i)
    {
        return endpoint((byte)i);
    }

    public static InetAddressAndPort endpoint(byte i)
    {
        try
        {
            return InetAddressAndPort.getByAddress(new byte[]{ 127, 0, 0, i });
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}
