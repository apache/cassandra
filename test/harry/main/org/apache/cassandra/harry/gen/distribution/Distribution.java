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

package org.apache.cassandra.harry.gen.distribution;

import org.apache.cassandra.harry.core.Configuration;

public interface Distribution
{
    Configuration.DistributionConfig toConfig();

    interface DistributionFactory
    {
        Distribution make();
    }

    long skew(long i);

    class IdentityDistribution implements Distribution
    {
        public Configuration.DistributionConfig toConfig()
        {
            return new Configuration.IdentityDistributionConfig();
        }

        public long skew(long i)
        {
            return i;
        }
    }

    class ConstantDistribution implements Distribution
    {
        public final long constant;

        public ConstantDistribution(long constant)
        {
            this.constant = constant;
        }

        public Configuration.DistributionConfig toConfig()
        {
            return new Configuration.ConstantDistributionConfig(constant);
        }

        public long skew(long i)
        {
            return constant;
        }

        public String toString()
        {
            return "ConstantDistribution{" +
                   "constant=" + constant +
                   '}';
        }
    }

    class ScaledDistribution implements Distribution
    {
        private final long min;
        private final long max;

        public ScaledDistribution(long min, long max)
        {
            this.min = min;
            this.max = max;
        }

        public Configuration.DistributionConfig toConfig()
        {
            return new Configuration.ScaledDistributionConfig(min, max);
        }

        public long skew(long i)
        {
            return scale(i, min, max);
        }

        public static long scale(long value, long min, long max)
        {
            if (value == 0)
                return (max - min) / 2;

            double nomalized = (1.0 * Math.abs(value)) / Long.MAX_VALUE;
            double diff = 0.5 * (max - min);
            if (value > 0)
                return (long) (min + nomalized * diff);
            else
                return (long) (max - nomalized * diff);
        }

        public String toString()
        {
            return "ScaledDistribution{" +
                   "min=" + min +
                   ", max=" + max +
                   '}';
        }
    }

    class NormalDistribution implements Distribution
    {
        private final org.apache.commons.math3.distribution.NormalDistribution delegate;

        public NormalDistribution()
        {
            delegate = new org.apache.commons.math3.distribution.NormalDistribution();
        }

        public Configuration.DistributionConfig toConfig()
        {
            return new Configuration.NormalDistributionConfig();
        }

        public long skew(long i)
        {
            return (long) delegate.cumulativeProbability((double) i);
        }
    }
}
