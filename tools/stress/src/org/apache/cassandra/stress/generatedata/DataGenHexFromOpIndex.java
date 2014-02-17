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


public class DataGenHexFromOpIndex extends DataGenHex
{

    final long minKey;
    final long maxKey;

    public DataGenHexFromOpIndex(long minKey, long maxKey)
    {
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    long next(long operationIndex)
    {
        long range = maxKey + 1 - minKey;
        return Math.abs((operationIndex % range) + minKey);
    }
}
