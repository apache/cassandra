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

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.db.marshal.BytesType;

public class HexBytes extends Generator<ByteBuffer>
{
    private final byte[] bytes;

    public HexBytes(String name, GeneratorConfig config)
    {
        super(BytesType.instance, config, name, ByteBuffer.class);
        bytes = new byte[(int) sizeDistribution.maxValue()];
    }

    @Override
    public ByteBuffer generate()
    {
        long seed = identityDistribution.next();
        sizeDistribution.setSeed(seed);
        int size = (int) sizeDistribution.next();
        for (int i = 0 ; i < size ; i +=16)
        {
            long value = identityDistribution.next();
            for (int j = 0 ; j < 16 && i + j < size ; j++)
            {
                int v = (int) (value & 15);
                bytes[i + j] = (byte) ((v < 10 ? '0' : 'A') + v);
                value >>>= 4;
            }
        }
        return ByteBuffer.wrap(Arrays.copyOf(bytes, size));
    }
}
