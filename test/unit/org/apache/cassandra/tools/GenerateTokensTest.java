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

package org.apache.cassandra.tools;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.tokenallocator.TokenAllocation;


public class GenerateTokensTest
{

    @Test
    public void testMain()
    {
        for (int rf = 1; rf <= 5; ++rf)
        {
            for (int numTokens = 1; numTokens <= 16 ; ++numTokens)
            {
                GenerateTokens.main(new String[]{"-n", "15", "-t", String.valueOf(numTokens), "--rf", String.valueOf(rf)});
                GenerateTokens.main(new String[]{"-n", "15", "-t", String.valueOf(numTokens), "--rf", String.valueOf(rf), "--partitioner", "Murmur3Partitioner"});
                GenerateTokens.main(new String[]{"-n", "15", "-t", String.valueOf(numTokens), "--rf", String.valueOf(rf), "--partitioner", "RandomPartitioner"});
                GenerateTokens.main(new String[]{"-n", "15", "-t", String.valueOf(numTokens), "--rf", String.valueOf(rf), "--racks", "15"});
                if (rf <= 2)
                    GenerateTokens.main(new String[]{"-n", "10", "-t", String.valueOf(numTokens), "--rf", String.valueOf(rf), "--racks", "5,5"});
                if (rf <= 3)
                    GenerateTokens.main(new String[]{"-n", "15", "-t", String.valueOf(numTokens), "--rf", String.valueOf(rf), "--racks", "5,5,5"});
                if (rf <= 4)
                    GenerateTokens.main(new String[]{"-n", "16", "-t", String.valueOf(numTokens), "--rf", String.valueOf(rf), "--racks", "4,4,4,4"});
                if (rf == 5)
                    GenerateTokens.main(new String[]{"-n", "15", "-t", String.valueOf(numTokens), "--rf", String.valueOf(rf), "--racks", "3,3,3,3,3"});
            }
        }
        Assert.assertFalse(LoggerFactory.getLogger(TokenAllocation.class).isWarnEnabled());
    }

}
