package org.apache.cassandra.stress.util;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class TimingIntervals
{
    final Map<String, TimingInterval> intervals;
    TimingIntervals(Iterable<String> opTypes)
    {
        long now = System.nanoTime();
        intervals = new TreeMap<>();
        for (String opType : opTypes)
            intervals.put(opType, new TimingInterval(now));
    }

    TimingIntervals(Map<String, TimingInterval> intervals)
    {
        this.intervals = intervals;
    }

    public TimingIntervals merge(TimingIntervals with, int maxSamples, long start)
    {
        assert intervals.size() == with.intervals.size();
        TreeMap<String, TimingInterval> ret = new TreeMap<>();

        for (String opType : intervals.keySet())
        {
            assert with.intervals.containsKey(opType);
            ret.put(opType, TimingInterval.merge(Arrays.asList(intervals.get(opType), with.intervals.get(opType)), maxSamples, start));
        }

        return new TimingIntervals(ret);
    }

    public TimingInterval get(String opType)
    {
        return intervals.get(opType);
    }

    public TimingInterval combine(int maxSamples)
    {
        long start = Long.MAX_VALUE;
        for (TimingInterval ti : intervals.values())
            start = Math.min(start, ti.startNanos());

        return TimingInterval.merge(intervals.values(), maxSamples, start);
    }

    public String str(TimingInterval.TimingParameter value)
    {
        return str(value, Float.NaN);
    }

    public String str(TimingInterval.TimingParameter value, float rank)
    {
        StringBuilder sb = new StringBuilder("[");

        for (Map.Entry<String, TimingInterval> entry : intervals.entrySet())
        {
            sb.append(entry.getKey());
            sb.append(":");
            sb.append(entry.getValue().getStringValue(value, rank));
            sb.append(", ");
        }

        sb.setLength(sb.length()-2);
        sb.append("]");

        return sb.toString();
    }

    public String opRates()
    {
        return str(TimingInterval.TimingParameter.OPRATE);
    }
    public String partitionRates()
    {
        return str(TimingInterval.TimingParameter.PARTITIONRATE);
    }
    public String rowRates()
    {
        return str(TimingInterval.TimingParameter.ROWRATE);
    }
    public String meanLatencies()
    {
        return str(TimingInterval.TimingParameter.MEANLATENCY);
    }
    public String maxLatencies()
    {
        return str(TimingInterval.TimingParameter.MAXLATENCY);
    }
    public String medianLatencies()
    {
        return str(TimingInterval.TimingParameter.MEDIANLATENCY);
    }
    public String rankLatencies(float rank)
    {
        return str(TimingInterval.TimingParameter.RANKLATENCY, rank);
    }
    public String errorCounts()
    {
        return str(TimingInterval.TimingParameter.ERRORCOUNT);
    }
    public String partitionCounts()
    {
        return str(TimingInterval.TimingParameter.PARTITIONCOUNT);
    }

    public long opRate()
    {
        long v = 0;
        for (TimingInterval interval : intervals.values())
            v += interval.opRate();
        return v;
    }

    public long startNanos()
    {
        long start = Long.MAX_VALUE;
        for (TimingInterval interval : intervals.values())
            start = Math.min(start, interval.startNanos());
        return start;
    }

    public long endNanos()
    {
        long end = Long.MIN_VALUE;
        for (TimingInterval interval : intervals.values())
            end = Math.max(end, interval.startNanos());
        return end;
    }

    public Map<String, TimingInterval> intervals()
    {
        return intervals;
    }
}
