package org.apache.cassandra.stress.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

// relatively simple timing class for getting a uniform sample of latencies, and saving other metrics
// ensures accuracy of timing by having single threaded timers that are check-pointed by the snapping thread,
// which waits for them to report back. They report back the data up to the last event prior to the check-point.
// if the threads are blocked/paused this may mean a period of time longer than the checkpoint elapses, but that all
// metrics calculated over the interval are accurate
public class Timing
{

    private final CopyOnWriteArrayList<Timer> timers = new CopyOnWriteArrayList<>();
    private volatile TimingInterval history;
    private final Random rnd = new Random();

    // TIMING

    private TimingInterval snapInterval(Random rnd) throws InterruptedException
    {
        final Timer[] timers = this.timers.toArray(new Timer[0]);
        final CountDownLatch ready = new CountDownLatch(timers.length);
        for (int i = 0 ; i < timers.length ; i++)
        {
            final Timer timer = timers[i];
            timer.requestReport(ready);
        }

        // TODO fail gracefully after timeout if a thread is stuck
        if (!ready.await(2L, TimeUnit.MINUTES))
            throw new RuntimeException("Timed out waiting for a timer thread - seems one got stuck");

        // reports have been filled in by timer threadCount, so merge
        List<TimingInterval> intervals = new ArrayList<>();
        for (Timer timer : timers)
            intervals.add(timer.report);

        return TimingInterval.merge(rnd, intervals, Integer.MAX_VALUE, history.endNanos());
    }

    // build a new timer and add it to the set of running timers
    public Timer newTimer()
    {
        final Timer timer = new Timer();
        timers.add(timer);
        return timer;
    }

    public void start()
    {
        history = new TimingInterval(System.nanoTime());
    }

    public TimingInterval snapInterval() throws InterruptedException
    {
        final TimingInterval interval = snapInterval(rnd);
        history = TimingInterval.merge(rnd, Arrays.asList(interval, history), 50000, history.startNanos());
        return interval;
    }

    public TimingInterval getHistory()
    {
        return history;
    }

}
