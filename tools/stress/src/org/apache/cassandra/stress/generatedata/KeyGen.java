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
import java.util.ArrayList;
import java.util.List;

public class KeyGen
{

    final DataGen dataGen;
    final int keySize;
    final List<ByteBuffer> keyBuffers = new ArrayList<>();

    public KeyGen(DataGen dataGen, int keySize)
    {
        this.dataGen = dataGen;
        this.keySize = keySize;
    }

    public List<ByteBuffer> getKeys(int n, long index)
    {
        while (keyBuffers.size() < n)
            keyBuffers.add(ByteBuffer.wrap(new byte[keySize]));
        dataGen.generate(keyBuffers, index, null);
        return keyBuffers;
    }

    public boolean isDeterministic()
    {
        return dataGen.isDeterministic();
    }

}
