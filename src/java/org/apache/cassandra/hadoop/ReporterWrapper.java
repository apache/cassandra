package org.apache.cassandra.hadoop;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.StatusReporter;

/**
 * A reporter that works with both mapred and mapreduce APIs.
 */
public class ReporterWrapper extends StatusReporter implements Reporter {
    private Reporter wrappedReporter;

    public ReporterWrapper(Reporter reporter) {
        wrappedReporter = reporter;
    }

    @Override
    public Counters.Counter getCounter(Enum<?> anEnum) {
        return wrappedReporter.getCounter(anEnum);
    }

    @Override
    public Counters.Counter getCounter(String s, String s1) {
        return wrappedReporter.getCounter(s, s1);
    }

    @Override
    public void incrCounter(Enum<?> anEnum, long l) {
        wrappedReporter.incrCounter(anEnum, l);
    }

    @Override
    public void incrCounter(String s, String s1, long l) {
        wrappedReporter.incrCounter(s, s1, l);
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
        return wrappedReporter.getInputSplit();
    }

    @Override
    public void progress() {
        wrappedReporter.progress();
    }

    // @Override
    public float getProgress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStatus(String s) {
        wrappedReporter.setStatus(s);
    }
}