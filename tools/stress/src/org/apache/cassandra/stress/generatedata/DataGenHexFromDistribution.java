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


import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

public class DataGenHexFromDistribution extends DataGenHex
{

    final Distribution distribution;

    public DataGenHexFromDistribution(Distribution distribution)
    {
        this.distribution = distribution;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    long next(long operationIndex)
    {
        return distribution.next();
    }

    public static DataGenHex buildGaussian(long minKey, long maxKey, double stdevsToLimit)
    {
        double midRange = (maxKey + minKey) / 2d;
        double halfRange = (maxKey - minKey) / 2d;
        return new DataGenHexFromDistribution(new DistributionBoundApache(new NormalDistribution(midRange, halfRange / stdevsToLimit), minKey, maxKey));
    }

    public static DataGenHex buildGaussian(long minKey, long maxKey, double mean, double stdev)
    {
        return new DataGenHexFromDistribution(new DistributionBoundApache(new NormalDistribution(mean, stdev), minKey, maxKey));
    }

    public static DataGenHex buildUniform(long minKey, long maxKey)
    {
        return new DataGenHexFromDistribution(new DistributionBoundApache(new UniformRealDistribution(minKey, maxKey), minKey, maxKey));
    }

}
