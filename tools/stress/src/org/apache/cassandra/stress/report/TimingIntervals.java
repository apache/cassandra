package org.apache.cassandra.stress.report;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class TimingIntervals
{
    final Map<String, TimingInterval> intervals;

    public TimingIntervals(Map<String, TimingInterval> intervals)
    {
        this.intervals = intervals;
    }

    public TimingInterval get(String opType)
    {
        return intervals.get(opType);
    }


    public String str(TimingInterval.TimingParameter value, String unit)
    {
        return str(value, Double.NaN, unit);
    }

    public String str(TimingInterval.TimingParameter value, double rank, String unit)
    {
        if (intervals.size() == 0)
        {
            return "[]";
        }

        StringBuilder sb = new StringBuilder("[");

        for (Map.Entry<String, TimingInterval> entry : intervals.entrySet())
        {
            sb.append(entry.getKey());
            sb.append(": ");
            sb.append(entry.getValue().getStringValue(value, rank));
            if (unit.length() > 0)
            {
                sb.append(" ");
                sb.append(unit);
            }
            sb.append(", ");
        }

        sb.setLength(sb.length()-2);
        sb.append("]");

        return sb.toString();
    }

    public String opRates()
    {
        return str(TimingInterval.TimingParameter.OPRATE, "op/s");
    }

    public String partitionRates()
    {
        return str(TimingInterval.TimingParameter.PARTITIONRATE, "pk/s");
    }

    public String rowRates()
    {
        return str(TimingInterval.TimingParameter.ROWRATE, "row/s");
    }

    public String meanLatencies()
    {
        return str(TimingInterval.TimingParameter.MEANLATENCY, "ms");
    }

    public String maxLatencies()
    {
        return str(TimingInterval.TimingParameter.MAXLATENCY, "ms");
    }

    public String medianLatencies()
    {
        return str(TimingInterval.TimingParameter.MEDIANLATENCY, "ms");
    }

    public String latenciesAtPercentile(double rank)
    {
        return str(TimingInterval.TimingParameter.RANKLATENCY, rank, "ms");
    }

    public String errorCounts()
    {
        return str(TimingInterval.TimingParameter.ERRORCOUNT, "");
    }

    public String partitionCounts()
    {
        return str(TimingInterval.TimingParameter.PARTITIONCOUNT, "");
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
