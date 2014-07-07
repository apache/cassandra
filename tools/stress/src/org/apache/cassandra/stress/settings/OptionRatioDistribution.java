package org.apache.cassandra.stress.settings;
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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.distribution.WeibullDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;

import org.apache.cassandra.stress.generate.Distribution;
import org.apache.cassandra.stress.generate.DistributionBoundApache;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.DistributionFixed;
import org.apache.cassandra.stress.generate.DistributionInverted;
import org.apache.cassandra.stress.generate.DistributionOffsetApache;
import org.apache.cassandra.stress.generate.RatioDistribution;
import org.apache.cassandra.stress.generate.RatioDistributionFactory;

/**
 * For selecting a mathematical distribution
 */
public class OptionRatioDistribution extends Option
{

    private static final Pattern FULL = Pattern.compile("(.*)/([0-9]+[KMB]?)", Pattern.CASE_INSENSITIVE);

    final OptionDistribution delegate;
    private double divisor;

    private static final RatioDistribution DEFAULT = new RatioDistribution(new DistributionFixed(1), 1);

    public OptionRatioDistribution(String prefix, String defaultSpec, String description)
    {
        delegate = new OptionDistribution(prefix, defaultSpec, description);
    }

    @Override
    public boolean accept(String param)
    {
        Matcher m = FULL.matcher(param);
        if (!m.matches() || !delegate.accept(m.group(1)))
            return false;
        divisor = OptionDistribution.parseLong(m.group(2));
        return true;
    }

    public static RatioDistributionFactory get(String spec)
    {
        OptionRatioDistribution opt = new OptionRatioDistribution("", "", "");
        if (!opt.accept(spec))
            throw new IllegalArgumentException();
        return opt.get();
    }

    public RatioDistributionFactory get()
    {
        return !delegate.setByUser() ? new DefaultFactory() : new DelegateFactory(delegate.get(), divisor);
    }

    @Override
    public boolean happy()
    {
        return delegate.happy();
    }

    public String longDisplay()
    {
        return delegate.longDisplay();
    }

    @Override
    public List<String> multiLineDisplay()
    {
        return Arrays.asList(
                GroupedOptions.formatMultiLine("EXP(min..max)/divisor", "An exponential ratio distribution over the range [min..max]/divisor"),
                GroupedOptions.formatMultiLine("EXTREME(min..max,shape)/divisor", "An extreme value (Weibull) ratio distribution over the range [min..max]/divisor"),
                GroupedOptions.formatMultiLine("GAUSSIAN(min..max,stdvrng)/divisor", "A gaussian/normal ratio distribution, where mean=(min+max)/2, and stdev is ((mean-min)/stdvrng)/divisor"),
                GroupedOptions.formatMultiLine("GAUSSIAN(min..max,mean,stdev)/divisor", "A gaussian/normal ratio distribution, with explicitly defined mean and stdev"),
                GroupedOptions.formatMultiLine("UNIFORM(min..max)/divisor", "A uniform ratio distribution over the range [min, max]/divisor"),
                GroupedOptions.formatMultiLine("FIXED(val)/divisor", "A fixed ratio distribution, always returning the same value"),
                "Preceding the name with ~ will invert the distribution, e.g. ~exp(1..10)/10 will yield 0.1 least, instead of most, often",
                "Aliases: extr, gauss, normal, norm, weibull"
        );
    }

    boolean setByUser()
    {
        return delegate.setByUser();
    }

    @Override
    public String shortDisplay()
    {
        return delegate.shortDisplay();
    }

    // factories

    private static final class DefaultFactory implements RatioDistributionFactory
    {
        @Override
        public RatioDistribution get()
        {
            return DEFAULT;
        }
    }

    private static final class DelegateFactory implements RatioDistributionFactory
    {
        final DistributionFactory delegate;
        final double divisor;

        private DelegateFactory(DistributionFactory delegate, double divisor)
        {
            this.delegate = delegate;
            this.divisor = divisor;
        }

        @Override
        public RatioDistribution get()
        {
            return new RatioDistribution(delegate.get(), divisor);
        }
    }

    @Override
    public int hashCode()
    {
        return delegate.hashCode();
    }

    @Override
    public boolean equals(Object that)
    {
        return super.equals(that) && ((OptionRatioDistribution) that).delegate.equals(this.delegate);
    }

}
