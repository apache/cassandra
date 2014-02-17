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


public class DistributionSeqBatch extends DataGenHex
{

    final Distribution delegate;
    final int batchSize;
    final long maxKey;

    private int batchIndex;
    private long batchKey;

    // object must be published safely if passed between threadCount, due to batchIndex not being volatile. various
    // hacks possible, but not ideal. don't want to use volatile as object intended for single threaded use.
    public DistributionSeqBatch(int batchSize, long maxKey, Distribution delegate)
    {
        this.batchIndex = batchSize;
        this.batchSize = batchSize;
        this.maxKey = maxKey;
        this.delegate = delegate;
    }

    @Override
    long next(long operationIndex)
    {
        if (batchIndex >= batchSize)
        {
            batchKey = delegate.next();
            batchIndex = 0;
        }
        long r = batchKey + batchIndex++;
        if (r > maxKey)
        {
            batchKey = delegate.next();
            batchIndex = 1;
            r = batchKey;
        }
        return r;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

}
