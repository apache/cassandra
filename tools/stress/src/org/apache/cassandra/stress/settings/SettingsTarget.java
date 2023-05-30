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

package org.apache.cassandra.stress.settings;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.stress.report.StressMetrics;
import org.apache.cassandra.stress.util.ResultLogger;

import static java.lang.String.format;
import static org.apache.cassandra.stress.settings.SettingsTarget.SettingsTargetType.LATENCY;
import static org.apache.cassandra.stress.settings.SettingsTarget.SettingsTargetType.SATURATION;

public class SettingsTarget implements Serializable
{
    public final double uncertainty;
    public final LatencyOptions latencyOptions;
    public final Target target;
    public final String targetType;

    enum SettingsTargetType
    {
        SATURATION,
        LATENCY
    }

    public SettingsTarget(TargetOptions options, double uncertainty)
    {
        this.uncertainty = uncertainty;
        latencyOptions = LatencyOptions.parse(options.percentiles.value());

        targetType = options.targetType.value();

        if (targetType.equals(SATURATION.name().toLowerCase()))
            target = new SaturationTarget(this);
        else if (targetType.equals(LATENCY.name().toLowerCase()))
            target = new LatencyTarget(this);
        else
            throw new IllegalStateException(format("There is no target of type %s", options.targetType));
    }

    public static SettingsTarget get(Map<String, String[]> clArgs, double uncertainty)
    {
        String[] params = clArgs.remove("-target");
        if (params == null)
            return new SettingsTarget(new SettingsTarget.TargetOptions(), uncertainty);

        GroupedOptions options = GroupedOptions.select(params, new SettingsTarget.TargetOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -target options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsTarget((SettingsTarget.TargetOptions) options, uncertainty);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-target", new SettingsTarget.TargetOptions());
    }

    public static Runnable helpPrinter()
    {
        return SettingsTarget::printHelp;
    }

    public void printSettings(ResultLogger out)
    {
        out.printf("  Type: %s%n", targetType);
        out.printf("  Latency Options: %s%n", latencyOptions.toString());
    }

    public static class LatencyOptions
    {
        private static final LatencyOptions DEFAULT_LATENCY_OPTIONS = new LatencyOptions(new double[][]{ { 0.95, 25 }, { 0.99, 50 }, { 0.999, 100 } });

        public final double[][] percentiles;

        public LatencyOptions(double[][] percentiles)
        {
            this.percentiles = percentiles;
        }

        public static LatencyOptions parse(String percentilesString)
        {
            String[] parsedPercentiles = percentilesString.split(",");

            List<double[]> percentiles = new ArrayList<>();

            for (String pair : parsedPercentiles)
            {
                if (!pair.contains("="))
                    continue;

                String trimmedPair = pair.trim();

                if (trimmedPair.isEmpty())
                    continue;

                String[] splitTrimmedPair = trimmedPair.split("=");

                if (splitTrimmedPair.length != 2)
                    continue;

                double percentile = Double.parseDouble(splitTrimmedPair[0].trim());
                double latency = Double.parseDouble(splitTrimmedPair[1].trim());

                percentiles.add(new double[]{ percentile, latency });
            }

            if (percentiles.isEmpty())
                return DEFAULT_LATENCY_OPTIONS;

            double latencies[][] = new double[percentiles.size()][2];

            for (int i = 0; i < percentiles.size(); i++)
                latencies[i] = percentiles.get(i);

            return new LatencyOptions(latencies);
        }

        @Override
        public String toString()
        {
            return "percentiles=" + Arrays.deepToString(percentiles);
        }
    }

    public static class TargetOptions extends GroupedOptions
    {
        final OptionSimple percentiles = new OptionSimple("percentiles=",
                                                          ".*",
                                                          "0.95=25,0.99=50,0.999=100",
                                                          "comma-separated list of percentiles and their max latencies in ms, e.g. '0.95=25,0.99=50,0.999=100'",
                                                          false);
        final OptionSimple targetType = new OptionSimple("type=",
                                                         "saturation|latency",
                                                         "saturation",
                                                         "type of target, can be either 'saturation' or 'latency', defaults to 'saturation'",
                                                         false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(percentiles, targetType);
        }
    }

    public static abstract class Target implements Serializable
    {
        protected final SettingsTarget settingsTarget;

        public Target(SettingsTarget settingsTarget)
        {
            this.settingsTarget = settingsTarget;
        }

        public abstract boolean satistfiesTarget(List<StressMetrics> metrics, int count, ResultLogger output);

        public boolean hasAverageImprovement(List<StressMetrics> results, int count, double minImprovement)
        {
            return results.size() < count + 1 || averageImprovement(results, count) >= minImprovement;
        }

        public double averageImprovement(List<StressMetrics> results, int count)
        {
            return 0;
        }
    }

    public static class SaturationTarget extends Target
    {
        public SaturationTarget(SettingsTarget settingsTarget)
        {
            super(settingsTarget);
        }

        @Override
        public boolean satistfiesTarget(List<StressMetrics> metrics, int count, ResultLogger output)
        {
            return hasAverageImprovement(metrics, count, 0)
                   && hasAverageImprovement(metrics, 5, settingsTarget.uncertainty);
        }

        @Override
        public double averageImprovement(List<StressMetrics> metrics, int count)
        {
            double improvement = 0;
            for (int i = metrics.size() - count; i < metrics.size(); i++)
            {
                double prev = metrics.get(i - 1).opRate();
                double cur = metrics.get(i).opRate();
                improvement += (cur - prev) / prev;
            }
            return improvement / count;
        }
    }

    public static class LatencyTarget extends Target
    {
        public LatencyTarget(SettingsTarget settingsTarget)
        {
            super(settingsTarget);
        }

        @Override
        public boolean satistfiesTarget(List<StressMetrics> metrics, int count, ResultLogger output)
        {
            double[][] percentiles = settingsTarget.latencyOptions.percentiles;

            for (double[] percentile : percentiles)
            {
                double current = metrics.get(metrics.size() - 1).latencyAtPercentileMs(percentile[0] * 100);
                double latencyTarget = percentile[1];

                if (current > latencyTarget)
                    return false;
            }

            return true;
        }
    }
}
