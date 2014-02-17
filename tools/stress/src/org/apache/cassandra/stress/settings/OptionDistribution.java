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


import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.stress.generatedata.*;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.distribution.WeibullDistribution;

/**
 * For selecting a mathematical distribution
 */
class OptionDistribution extends Option
{

    private static final Pattern FULL = Pattern.compile("([A-Z]+)\\((.+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern ARGS = Pattern.compile("[^,]+");

    final String prefix;
    private String spec;
    private final String defaultSpec;

    public OptionDistribution(String prefix, String defaultSpec)
    {
        this.prefix = prefix;
        this.defaultSpec = defaultSpec;
    }

    @Override
    public boolean accept(String param)
    {
        if (!param.toLowerCase().startsWith(prefix))
            return false;
        spec = param.substring(prefix.length());
        return true;
    }

    private static DistributionFactory get(String spec)
    {
        Matcher m = FULL.matcher(spec);
        if (!m.matches())
            throw new IllegalArgumentException("Illegal distribution specification: " + spec);
        String name = m.group(1);
        Impl impl = LOOKUP.get(name.toLowerCase());
        if (impl == null)
            throw new IllegalArgumentException("Illegal distribution type: " + name);
        List<String> params = new ArrayList<>();
        m = ARGS.matcher(m.group(2));
        while (m.find())
            params.add(m.group());
        return impl.getFactory(params);
    }

    public DistributionFactory get()
    {
        return spec != null ? get(spec) : get(defaultSpec);
    }

    @Override
    public boolean happy()
    {
        return spec != null || defaultSpec != null;
    }

    public String longDisplay()
    {
        return shortDisplay() + ": Specify a mathematical distribution";
    }

    @Override
    public List<String> multiLineDisplay()
    {
        return Arrays.asList(
                GroupedOptions.formatMultiLine("EXP(min..max)", "An exponential distribution over the range [min..max]"),
                GroupedOptions.formatMultiLine("EXTREME(min..max,shape)", "An extreme value (Weibull) distribution over the range [min..max]"),
                GroupedOptions.formatMultiLine("GAUSSIAN(min..max,stdvrng)", "A gaussian/normal distribution, where mean=(min+max)/2, and stdev is (mean-min)/stdvrng"),
                GroupedOptions.formatMultiLine("GAUSSIAN(min..max,mean,stdev)", "A gaussian/normal distribution, with explicitly defined mean and stdev"),
                GroupedOptions.formatMultiLine("UNIFORM(min..max)", "A uniform distribution over the range [min, max]"),
                GroupedOptions.formatMultiLine("FIXED(val)", "A fixed distribution, always returning the same value"),
                "Aliases: extr, gauss, normal, norm, weibull"
        );
    }

    @Override
    public String shortDisplay()
    {
        return prefix + "DIST(?)";
    }

    private static final Map<String, Impl> LOOKUP;
    static
    {
        final Map<String, Impl> lookup = new HashMap<>();
        lookup.put("exp", new ExponentialImpl());
        lookup.put("extr", new ExtremeImpl());
        lookup.put("extreme", lookup.get("extreme"));
        lookup.put("weibull", lookup.get("weibull"));
        lookup.put("gaussian", new GaussianImpl());
        lookup.put("normal", lookup.get("gaussian"));
        lookup.put("gauss", lookup.get("gaussian"));
        lookup.put("norm", lookup.get("gaussian"));
        lookup.put("uniform", new UniformImpl());
        lookup.put("fixed", new FixedImpl());
        LOOKUP = lookup;
    }

    // factory builders

    private static interface Impl
    {
        public DistributionFactory getFactory(List<String> params);
    }

    private static final class GaussianImpl implements Impl
    {

        @Override
        public DistributionFactory getFactory(List<String> params)
        {
            if (params.size() > 3 || params.size() < 1)
                throw new IllegalArgumentException("Invalid parameter list for gaussian distribution: " + params);
            try
            {
                String[] bounds = params.get(0).split("\\.\\.+");
                final long min = Long.parseLong(bounds[0]);
                final long max = Long.parseLong(bounds[1]);
                final double mean, stdev;
                if (params.size() == 3)
                {
                    mean = Double.parseDouble(params.get(1));
                    stdev = Double.parseDouble(params.get(2));
                }
                else
                {
                    final double stdevsToEdge = params.size() == 1 ? 3d : Double.parseDouble(params.get(1));
                    mean = (min + max) / 2d;
                    stdev = ((max - min) / 2d) / stdevsToEdge;
                }
                return new GaussianFactory(min, max, mean, stdev);
            } catch (Exception _)
            {
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            }
        }
    }

    private static final class ExponentialImpl implements Impl
    {
        @Override
        public DistributionFactory getFactory(List<String> params)
        {
            if (params.size() != 1)
                throw new IllegalArgumentException("Invalid parameter list for gaussian distribution: " + params);
            try
            {
                String[] bounds = params.get(0).split("\\.\\.+");
                final long min = Long.parseLong(bounds[0]);
                final long max = Long.parseLong(bounds[1]);
                ExponentialDistribution findBounds = new ExponentialDistribution(1d);
                // max probability should be roughly equal to accuracy of (max-min) to ensure all values are visitable,
                // over entire range, but this results in overly skewed distribution, so take sqrt
                final double mean = (max - min) / findBounds.inverseCumulativeProbability(1d - Math.sqrt(1d/(max-min)));
                return new ExpFactory(min, max, mean);
            } catch (Exception _)
            {
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            }
        }
    }

    private static final class ExtremeImpl implements Impl
    {
        @Override
        public DistributionFactory getFactory(List<String> params)
        {
            if (params.size() != 2)
                throw new IllegalArgumentException("Invalid parameter list for extreme (Weibull) distribution: " + params);
            try
            {
                String[] bounds = params.get(0).split("\\.\\.+");
                final long min = Long.parseLong(bounds[0]);
                final long max = Long.parseLong(bounds[1]);
                final double shape = Double.parseDouble(params.get(1));
                WeibullDistribution findBounds = new WeibullDistribution(shape, 1d);
                // max probability should be roughly equal to accuracy of (max-min) to ensure all values are visitable,
                // over entire range, but this results in overly skewed distribution, so take sqrt
                final double scale = (max - min) / findBounds.inverseCumulativeProbability(1d - Math.sqrt(1d/(max-min)));
                return new ExtremeFactory(min, max, shape, scale);
            } catch (Exception _)
            {
                throw new IllegalArgumentException("Invalid parameter list for extreme (Weibull) distribution: " + params);
            }
        }
    }

    private static final class UniformImpl implements Impl
    {

        @Override
        public DistributionFactory getFactory(List<String> params)
        {
            if (params.size() != 1)
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            try
            {
                String[] bounds = params.get(0).split("\\.\\.+");
                final long min = Long.parseLong(bounds[0]);
                final long max = Long.parseLong(bounds[1]);
                return new UniformFactory(min, max);
            } catch (Exception _)
            {
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            }
        }
    }

    private static final class FixedImpl implements Impl
    {

        @Override
        public DistributionFactory getFactory(List<String> params)
        {
            if (params.size() != 1)
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            try
            {
                final long key = Long.parseLong(params.get(0));
                return new FixedFactory(key);
            } catch (Exception _)
            {
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            }
        }
    }

    // factories

    private static final class ExpFactory implements DistributionFactory
    {
        final long min, max;
        final double mean;
        private ExpFactory(long min, long max, double mean)
        {
            this.min = min;
            this.max = max;
            this.mean = mean;
        }

        @Override
        public Distribution get()
        {
            return new DistributionOffsetApache(new ExponentialDistribution(mean), min, max);
        }
    }

    private static final class ExtremeFactory implements DistributionFactory
    {
        final long min, max;
        final double shape, scale;
        private ExtremeFactory(long min, long max, double shape, double scale)
        {
            this.min = min;
            this.max = max;
            this.shape = shape;
            this.scale = scale;
        }

        @Override
        public Distribution get()
        {
            return new DistributionOffsetApache(new WeibullDistribution(shape, scale), min, max);
        }
    }

    private static final class GaussianFactory implements DistributionFactory
    {
        final long min, max;
        final double mean, stdev;
        private GaussianFactory(long min, long max, double mean, double stdev)
        {
            this.min = min;
            this.max = max;
            this.stdev = stdev;
            this.mean = mean;
        }

        @Override
        public Distribution get()
        {
            return new DistributionBoundApache(new NormalDistribution(mean, stdev), min, max);
        }
    }

    private static final class UniformFactory implements DistributionFactory
    {
        final long min, max;
        private UniformFactory(long min, long max)
        {
            this.min = min;
            this.max = max;
        }

        @Override
        public Distribution get()
        {
            return new DistributionBoundApache(new UniformRealDistribution(min, max), min, max);
        }
    }

    private static final class FixedFactory implements DistributionFactory
    {
        final long key;
        private FixedFactory(long key)
        {
            this.key = key;
        }

        @Override
        public Distribution get()
        {
            return new DistributionFixed(key);
        }
    }

    @Override
    public int hashCode()
    {
        return prefix.hashCode();
    }

    @Override
    public boolean equals(Object that)
    {
        return super.equals(that) && ((OptionDistribution) that).prefix.equals(this.prefix);
    }

}
