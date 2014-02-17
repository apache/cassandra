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
import java.util.List;

/**
 * Generates a row of data, by constructing one byte buffers per column according to some algorithm
 * and delegating the work of populating the values of those byte buffers to the provided data generator
 */
public abstract class RowGen
{

    final DataGen dataGen;
    protected RowGen(DataGen dataGenerator)
    {
        this.dataGen = dataGenerator;
    }

    public List<ByteBuffer> generate(long operationIndex, ByteBuffer key)
    {
        List<ByteBuffer> fill = getColumns(operationIndex);
        dataGen.generate(fill, operationIndex, key);
        return fill;
    }

    // these byte[] may be re-used
    abstract List<ByteBuffer> getColumns(long operationIndex);

    abstract public boolean isDeterministic();

}
