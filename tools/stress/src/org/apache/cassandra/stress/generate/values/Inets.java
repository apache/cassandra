/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.stress.generate.values;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.cassandra.db.marshal.InetAddressType;


public class Inets extends Generator<InetAddress>
{
    final byte[] buf;
    public Inets(String name, GeneratorConfig config)
    {
        super(InetAddressType.instance, config, name, InetAddress.class);
        buf = new byte[4];
    }

    @Override
    public InetAddress generate()
    {
        int val = (int) identityDistribution.next();

        buf[0] = (byte)(val >>> 24);
        buf[1] = (byte)(val >>> 16);
        buf[2] = (byte)(val >>> 8);
        buf[3] = (byte)val;

        try
        {
            return InetAddress.getByAddress(buf);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}
