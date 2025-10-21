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

package org.apache.cassandra.metrics;

import java.util.function.DoubleSupplier;
import java.util.function.ToDoubleFunction;

import com.codahale.metrics.Metered;
import com.codahale.metrics.RatioGauge;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class RatioGaugeSet
{
    public static final String ONE_MINUTE = "OneMinute";
    public static final String FIVE_MINUTE = "FiveMinute";
    public static final String FIFTEEN_MINUTE = "FifteenMinute";
    public static final String MEAN_RATIO = "";

    public final RatioGauge oneMinute;
    public final RatioGauge fiveMinute;
    public final RatioGauge fifteenMinute;
    public final RatioGauge mean;

    public RatioGaugeSet(Metered numerator, Metered denominator, MetricNameFactory factory, String namePattern)
    {
        this.oneMinute = ratioGauge(factory.createMetricName(String.format(namePattern, ONE_MINUTE)), numerator::getOneMinuteRate, denominator::getOneMinuteRate);
        this.fiveMinute = ratioGauge(factory.createMetricName(String.format(namePattern, FIVE_MINUTE)), numerator::getFiveMinuteRate, denominator::getFiveMinuteRate);
        this.fifteenMinute = ratioGauge(factory.createMetricName(String.format(namePattern, FIFTEEN_MINUTE)), numerator::getFifteenMinuteRate, denominator::getFifteenMinuteRate);
        this.mean = ratioGauge(factory.createMetricName(String.format(namePattern, MEAN_RATIO)), numerator::getCount, denominator::getCount);
    }

    private static RatioGauge ratioGauge(DoubleSupplier numerator, DoubleSupplier denominator)
    {
        return new RatioGauge()
        {
            protected Ratio getRatio()
            {
                return Ratio.of(numerator.getAsDouble(), denominator.getAsDouble());
            }
        };
    }

    private RatioGauge ratioGauge(MetricName name, DoubleSupplier numerator, DoubleSupplier denominator)
    {
        return Metrics.register(name, ratioGauge(numerator, denominator));
    }

    public static Metered sum(Metered... meters)
    {
        return new SummingMeter(meters);
    }

    private static class SummingMeter implements Metered
    {
        private final Metered[] meters;

        public SummingMeter(Metered... meters)
        {
            this.meters = meters;
        }

        @Override
        public long getCount()
        {
            long count = 0;
            for (Metered meter : meters)
                count += meter.getCount();
            return count;
        }

        private double getRate(ToDoubleFunction<Metered> rateSupplier)
        {
            double rate = 0;
            for (Metered meter : meters)
                rate += rateSupplier.applyAsDouble(meter);
            return rate;
        }

        @Override
        public double getMeanRate()
        {
            return getRate(Metered::getMeanRate);
        }

        @Override
        public double getFifteenMinuteRate()
        {
            return getRate(Metered::getFifteenMinuteRate);
        }

        @Override
        public double getFiveMinuteRate()
        {
            return getRate(Metered::getFiveMinuteRate);
        }

        @Override
        public double getOneMinuteRate()
        {
            return getRate(Metered::getOneMinuteRate);
        }
    }
}
