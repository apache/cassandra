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

package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;

public class LegacyCardinality implements ICardinality<com.clearspring.analytics.stream.cardinality.ICardinality>
{
    private final com.clearspring.analytics.stream.cardinality.ICardinality legacyCardinality;

    public LegacyCardinality(com.clearspring.analytics.stream.cardinality.ICardinality legacyCardinality)
    {
        this.legacyCardinality = legacyCardinality;
    }

    @Override
    public void offerHashed(long hashed)
    {
        legacyCardinality.offerHashed(hashed);
    }

    @Override
    public long cardinality()
    {
        return legacyCardinality.cardinality();
    }

    @Override
    public int sizeof()
    {
        return legacyCardinality.sizeof();
    }

    @Override
    public byte[] getBytes() throws IOException
    {
        return legacyCardinality.getBytes();
    }

    @Override
    public com.clearspring.analytics.stream.cardinality.ICardinality getCardinality()
    {
        return legacyCardinality;
    }

    @Override
    public ICardinality<com.clearspring.analytics.stream.cardinality.ICardinality> merge(com.clearspring.analytics.stream.cardinality.ICardinality cardinality) throws CardinalityMergeException
    {
        try
        {
            return new LegacyCardinality(legacyCardinality.merge(cardinality));
        }
        catch (com.clearspring.analytics.stream.cardinality.CardinalityMergeException ex)
        {
            throw new CardinalityMergeException(ex);
        }
    }
}
