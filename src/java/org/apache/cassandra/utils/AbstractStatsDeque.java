package org.apache.cassandra.utils;

import java.util.Iterator;
import java.util.ArrayDeque;

public abstract class AbstractStatsDeque implements Iterable<Double>
{
    public abstract Iterator<Double> iterator();
    public abstract int size();
    public abstract void add(double o);
    public abstract void clear();

    //
    // statistical methods
    //

    public double sum()
    {
        double sum = 0d;
        for (Double interval : this)
        {
            sum += interval;
        }
        return sum;
    }

    public double sumOfDeviations()
    {
        double sumOfDeviations = 0d;
        double mean = mean();

        for (Double interval : this)
        {
            double v = interval - mean;
            sumOfDeviations += v * v;
        }

        return sumOfDeviations;
    }

    public double mean()
    {
        return sum() / size();
    }

    public double variance()
    {
        return sumOfDeviations() / size();
    }

    public double stdev()
    {
        return Math.sqrt(variance());
    }
}
