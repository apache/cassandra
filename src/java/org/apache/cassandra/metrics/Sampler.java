package org.apache.cassandra.metrics;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.utils.Clock;

import com.google.common.annotations.VisibleForTesting;

public abstract class Sampler<T>
{
    public enum SamplerType
    {
        READS, WRITES, LOCAL_READ_TIME, WRITE_SIZE, CAS_CONTENTIONS
    }

    @VisibleForTesting
    Clock clock = Clock.instance;

    @VisibleForTesting
    static final ThreadPoolExecutor samplerExecutor = new JMXEnabledThreadPoolExecutor(1, 1,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(1000),
            new NamedThreadFactory("Sampler"),
            "internal");

    static
    {
        samplerExecutor.setRejectedExecutionHandler((runnable, executor) ->
        {
            MessagingService.instance().incrementDroppedMessages(Verb._SAMPLE);
        });
    }

    public void addSample(final T item, final int value)
    {
        if (isEnabled())
            samplerExecutor.submit(() -> insert(item, value));
    }

    protected abstract void insert(T item, long value);

    public abstract boolean isEnabled();

    public abstract void beginSampling(int capacity, int durationMillis);

    public abstract List<Sample<T>> finishSampling(int count);

    public abstract String toString(T value);

    /**
     * Represents the ranked items collected during a sample period
     */
    public static class Sample<S> implements Serializable
    {
        public final S value;
        public final long count;
        public final long error;

        public Sample(S value, long count, long error)
        {
            this.value = value;
            this.count = count;
            this.error = error;
        }

        @Override
        public String toString()
        {
            return "Sample [value=" + value + ", count=" + count + ", error=" + error + "]";
        }
    }
}
