package org.apache.cassandra.db.marshal;
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


import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongType extends AbstractType
{
    public static final LongType instance = new LongType();

    LongType() {} // singleton

    public int compare(byte[] o1, byte[] o2)
    {
        if (o1.length == 0)
        {
            return o2.length == 0 ? 0 : -1;
        }
        if (o2.length == 0)
        {
            return 1;
        }

        long L1 = ByteBuffer.wrap(o1).getLong();
        long L2 = ByteBuffer.wrap(o2).getLong();
        return Long.valueOf(L1).compareTo(Long.valueOf(L2));
    }

    public String getString(byte[] bytes)
    {
        if (bytes.length == 0)
        {
            return "";
        }
        if (bytes.length != 8)
        {
            throw new MarshalException("A long is exactly 8 bytes");
        }
        return String.valueOf(ByteBuffer.wrap(bytes).getLong());
    }
}
