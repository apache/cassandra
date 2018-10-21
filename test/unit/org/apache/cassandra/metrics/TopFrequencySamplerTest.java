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
package org.apache.cassandra.metrics;

import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.metrics.Sampler.Sample;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TopFrequencySamplerTest extends SamplerTest
{
    @Before
    public void setSampler()
    {
        this.sampler = new FrequencySampler<String>()
        {
            public String toString(String value)
            {
                return value;
            }
        };
    }

    @Test
    public void testSamplerSingleInsertionsEqualMulti() throws TimeoutException
    {
        sampler.beginSampling(10, 100000);
        insert(sampler);
        waitForEmpty(1000);
        List<Sample<String>> single = sampler.finishSampling(10);

        FrequencySampler<String> sampler2 = new FrequencySampler<String>()
        {
            public String toString(String value)
            {
                return value;
            }
        };
        sampler2.beginSampling(10, 100000);
        for(int i = 1; i <= 10; i++)
        {
           String key = "item" + i;
           sampler2.addSample(key, i);
        }
        waitForEmpty(1000);
        Assert.assertEquals(countMap(single), countMap(sampler2.finishSampling(10)));
    }


}
