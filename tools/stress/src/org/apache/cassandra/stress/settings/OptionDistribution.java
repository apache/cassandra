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

import com.google.common.base.Function;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.distribution.WeibullDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;

import org.apache.cassandra.stress.generate.*;

/**
 * For selecting a mathematical distribution
 */
public class OptionDistribution extends Option
{

    public static final Function<String, DistributionFactory> BUILDER = new Function<String, DistributionFactory>()
    {
        public DistributionFactory apply(String s)
        {
            return get(s);
        }
    };

    private static final Pattern FULL = Pattern.compile("(~?)([A-Z]+)\\((.+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern ARGS = Pattern.compile("[^,]+");

    final String prefix;
    private String spec;
    private final String defaultSpec;
    private final String description;
    private final boolean required;

    public OptionDistribution(String prefix, String defaultSpec, String description)
    {
        this(prefix, defaultSpec, description, defaultSpec == null);
    }

    public OptionDistribution(String prefix, String defaultSpec, String description, boolean required)
    {
        this.prefix = prefix;
        this.defaultSpec = defaultSpec;
        this.description = description;
        this.required = required;
    }

    @Override
    public boolean accept(String param)
    {
        if (!param.toLowerCase().startsWith(prefix))
            return false;
        spec = param.substring(prefix.length());
        return true;
    }

    public static DistributionFactory get(String spec)
    {
        Matcher m = FULL.matcher(spec);
        if (!m.matches())
            throw new IllegalArgumentException("Illegal distribution specification: " + spec);
        boolean inverse = m.group(1).equals("~");
        String name = m.group(2);
        Impl impl = LOOKUP.get(name.toLowerCase());
        if (impl == null)
            throw new IllegalArgumentException("Illegal distribution type: " + name);
        List<String> params = new ArrayList<>();
        m = ARGS.matcher(m.group(3));
        while (m.find())
            params.add(m.group());
        DistributionFactory factory = impl.getFactory(params);
        return inverse ? new InverseFactory(factory) : factory;
    }

    public DistributionFactory get()
    {
        return spec != null ? get(spec) : defaultSpec != null ? get(defaultSpec) : null;
    }

    @Override
    public boolean happy()
    {
        return !required || spec != null;
    }

    public String longDisplay()
    {
        return shortDisplay() + ": " + description;
    }

    @Override
    public List<String> multiLineDisplay()
    {
        return Arrays.asList(
                GroupedOptions.formatMultiLine("EXP(min..max)", "An exponential distribution over the range [min..max]"),
                GroupedOptions.formatMultiLine("EXTREME(min..max,shape)", "An extreme value (Weibull) distribution over the range [min..max]"),
                GroupedOptions.formatMultiLine("QEXTREME(min..max,shape,quantas)", "An extreme value, split into quantas, within which the chance of selection is uniform"),
                GroupedOptions.formatMultiLine("GAUSSIAN(min..max,stdvrng)", "A gaussian/normal distribution, where mean=(min+max)/2, and stdev is (mean-min)/stdvrng"),
                GroupedOptions.formatMultiLine("GAUSSIAN(min..max,mean,stdev)", "A gaussian/normal distribution, with explicitly defined mean and stdev"),
                GroupedOptions.formatMultiLine("UNIFORM(min..max)", "A uniform distribution over the range [min, max]"),
                GroupedOptions.formatMultiLine("FIXED(val)", "A fixed distribution, always returning the same value"),
                "Preceding the name with ~ will invert the distribution, e.g. ~exp(1..10) will yield 10 most, instead of least, often",
                "Aliases: extr, qextr, gauss, normal, norm, weibull"
        );
    }

    boolean setByUser()
    {
        return spec != null;
    }

    boolean present()
    {
        return setByUser() || defaultSpec != null;
    }

    @Override
    public String shortDisplay()
    {
        return (defaultSpec != null ? "[" : "") + prefix + "DIST(?)" + (defaultSpec != null ? "]" : "");
    }

    private static final Map<String, Impl> LOOKUP;
    static
    {
        final Map<String, Impl> lookup = new HashMap<>();
        lookup.put("exp", new ExponentialImpl());
        lookup.put("extr", new ExtremeImpl());
        lookup.put("qextr", new QuantizedExtremeImpl());
        lookup.put("extreme", lookup.get("extr"));
        lookup.put("qextreme", lookup.get("qextr"));
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

    public static long parseLong(String value)
    {
        long multiplier = 1;
        value = value.trim().toLowerCase();
        switch (value.charAt(value.length() - 1))
        {
            case 'b':
                multiplier *= 1000;
            case 'm':
                multiplier *= 1000;
            case 'k':
                multiplier *= 1000;
                value = value.substring(0, value.length() - 1);
        }
        return Long.parseLong(value) * multiplier;
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
                final long min = parseLong(bounds[0]);
                final long max = parseLong(bounds[1]);
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
                if (min == max)
                    return new FixedFactory(min);
                return new GaussianFactory(min, max, mean, stdev);
            } catch (Exception ignore)
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
                final long min = parseLong(bounds[0]);
                final long max = parseLong(bounds[1]);
                if (min == max)
                    return new FixedFactory(min);
                ExponentialDistribution findBounds = new ExponentialDistribution(1d);
                // max probability should be roughly equal to accuracy of (max-min) to ensure all values are visitable,
                // over entire range, but this results in overly skewed distribution, so take sqrt
                final double mean = (max - min) / findBounds.inverseCumulativeProbability(1d - Math.sqrt(1d/(max-min)));
                return new ExpFactory(min, max, mean);
            } catch (Exception ignore)
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
                final long min = parseLong(bounds[0]);
                final long max = parseLong(bounds[1]);
                if (min == max)
                    return new FixedFactory(min);
                final double shape = Double.parseDouble(params.get(1));
                WeibullDistribution findBounds = new WeibullDistribution(shape, 1d);
                // max probability should be roughly equal to accuracy of (max-min) to ensure all values are visitable,
                // over entire range, but this results in overly skewed distribution, so take sqrt
                final double scale = (max - min) / findBounds.inverseCumulativeProbability(1d - Math.sqrt(1d/(max-min)));
                return new ExtremeFactory(min, max, shape, scale);
            } catch (Exception ignore)
            {
                throw new IllegalArgumentException("Invalid parameter list for extreme (Weibull) distribution: " + params);
            }
        }
    }

    private static final class QuantizedExtremeImpl implements Impl
    {
        @Override
        public DistributionFactory getFactory(List<String> params)
        {
            if (params.size() != 3)
                throw new IllegalArgumentException("Invalid parameter list for quantized extreme (Weibull) distribution: " + params);
            try
            {
                String[] bounds = params.get(0).split("\\.\\.+");
                final long min = parseLong(bounds[0]);
                final long max = parseLong(bounds[1]);
                final double shape = Double.parseDouble(params.get(1));
                final int quantas = Integer.parseInt(params.get(2));
                WeibullDistribution findBounds = new WeibullDistribution(shape, 1d);
                // max probability should be roughly equal to accuracy of (max-min) to ensure all values are visitable,
                // over entire range, but this results in overly skewed distribution, so take sqrt
                final double scale = (max - min) / findBounds.inverseCumulativeProbability(1d - Math.sqrt(1d/(max-min)));
                if (min == max)
                    return new FixedFactory(min);
                return new QuantizedExtremeFactory(min, max, shape, scale, quantas);
            } catch (Exception ignore)
            {
                throw new IllegalArgumentException("Invalid parameter list for quantized extreme (Weibull) distribution: " + params);
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
                final long min = parseLong(bounds[0]);
                final long max = parseLong(bounds[1]);
                if (min == max)
                    return new FixedFactory(min);
                return new UniformFactory(min, max);
            } catch (Exception ignore)
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
                final long key = parseLong(params.get(0));
                return new FixedFactory(key);
            } catch (Exception ignore)
            {
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            }
        }
    }

    private static final class InverseFactory implements DistributionFactory
    {
        final DistributionFactory wrapped;
        private InverseFactory(DistributionFactory wrapped)
        {
            this.wrapped = wrapped;
        }

        public Distribution get()
        {
            return new DistributionInverted(wrapped.get());
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
            return new DistributionOffsetApache(new ExponentialDistribution(new JDKRandomGenerator(), mean, ExponentialDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY), min, max);
        }
    }

    private static class ExtremeFactory implements DistributionFactory
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
            return new DistributionOffsetApache(new WeibullDistribution(new JDKRandomGenerator(), shape, scale, WeibullDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY), min, max);
        }
    }

    private static final class QuantizedExtremeFactory extends ExtremeFactory
    {
        final int quantas;
        private QuantizedExtremeFactory(long min, long max, double shape, double scale, int quantas)
        {
            super(min, max, shape, scale);
            this.quantas = quantas;
        }

        @Override
        public Distribution get()
        {
            return new DistributionQuantized(new DistributionOffsetApache(new WeibullDistribution(new JDKRandomGenerator(), shape, scale, WeibullDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY), min, max), quantas);
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
            return new DistributionBoundApache(new NormalDistribution(new JDKRandomGenerator(), mean, stdev, NormalDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY), min, max);
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
            return new DistributionBoundApache(new UniformRealDistribution(new JDKRandomGenerator(), min, max + 1), min, max);
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
