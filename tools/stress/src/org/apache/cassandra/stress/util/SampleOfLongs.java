package org.apache.cassandra.stress.util;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

// represents a sample of long (latencies) together with the probability of selection of each sample (i.e. the ratio of
// samples to total number of events). This is used to ensure that, when merging, the result has samples from each
// with equal probability
public final class SampleOfLongs
{

    // nanos
    final long[] sample;

    // probability with which each sample was selected
    final double p;

    SampleOfLongs(long[] sample, int p)
    {
        this.sample = sample;
        this.p = 1 / (float) p;
    }

    SampleOfLongs(long[] sample, double p)
    {
        this.sample = sample;
        this.p = p;
    }

    static SampleOfLongs merge(Random rnd, List<SampleOfLongs> merge, int maxSamples)
    {
        int maxLength = 0;
        double targetp = 1;
        for (SampleOfLongs sampleOfLongs : merge)
        {
            maxLength += sampleOfLongs.sample.length;
            targetp = Math.min(targetp, sampleOfLongs.p);
        }
        long[] sample = new long[maxLength];
        int count = 0;
        for (SampleOfLongs latencies : merge)
        {
            long[] in = latencies.sample;
            double p = targetp / latencies.p;
            for (int i = 0 ; i < in.length ; i++)
                if (rnd.nextDouble() < p)
                    sample[count++] = in[i];
        }
        if (count > maxSamples)
        {
            targetp = subsample(rnd, maxSamples, sample, count, targetp);
            count = maxSamples;
        }
        sample = Arrays.copyOf(sample, count);
        Arrays.sort(sample);
        return new SampleOfLongs(sample, targetp);
    }

    public SampleOfLongs subsample(Random rnd, int maxSamples)
    {
        if (maxSamples > sample.length)
            return this;

        long[] sample = this.sample.clone();
        double p = subsample(rnd, maxSamples, sample, sample.length, this.p);
        sample = Arrays.copyOf(sample, maxSamples);
        return new SampleOfLongs(sample, p);
    }

    private static double subsample(Random rnd, int maxSamples, long[] sample, int count, double p)
    {
        // want exactly maxSamples, so select random indexes up to maxSamples
        for (int i = 0 ; i < maxSamples ; i++)
        {
            int take = i + rnd.nextInt(count - i);
            long tmp = sample[i];
            sample[i] = sample[take];
            sample[take] = tmp;
        }

        // calculate new p; have selected with probability maxSamples / count
        // so multiply p by this probability
        p *= maxSamples / (double) sample.length;
        return p;
    }

    public double medianLatency()
    {
        if (sample.length == 0)
            return 0;
        return sample[sample.length >> 1] * 0.000001d;
    }

    // 0 < rank < 1
    public double rankLatency(float rank)
    {
        if (sample.length == 0)
            return 0;
        int index = (int)(rank * sample.length);
        if (index >= sample.length)
            index = sample.length - 1;
        return sample[index] * 0.000001d;
    }

}

