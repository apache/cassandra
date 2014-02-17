package org.apache.cassandra.stress.generatedata;
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

public abstract class DataGenHex extends DataGen
{

    abstract long next(long operationIndex);

    @Override
    public final void generate(ByteBuffer fill, long operationIndex, ByteBuffer seed)
    {
        fill.clear();
        fillKeyStringBytes(next(operationIndex), fill.array());
    }

    public static void fillKeyStringBytes(long key, byte[] fill)
    {
        int ub = fill.length - 1;
        int offset = 0;
        while (key != 0)
        {
            int digit = ((int) key) & 15;
            key >>>= 4;
            fill[ub - offset++] = digit(digit);
        }
        while (offset < fill.length)
            fill[ub - offset++] = '0';
    }

    // needs to be UTF-8, but for these chars there is no difference
    private static byte digit(int num)
    {
        if (num < 10)
            return (byte)('0' + num);
        return (byte)('A' + (num - 10));
    }

}
