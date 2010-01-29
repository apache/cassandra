package org.apache.cassandra.cache;

public interface JMXInstrumentedCacheMBean
{
    public int getCapacity();
    public void setCapacity(int capacity);
    public int getSize();

    /** total request count since cache creation */
    public long getRequests();

    /** total cache hit count since cache creation */
    public long getHits();

    /**
     * hits / requests since the last time getHitRate was called.  serious telemetry apps should not use this,
     * and should instead track the deltas from getHits / getRequests themselves, since those will not be
     * affected by multiple users calling it.  Provided for convenience only.
     */
    public double getRecentHitRate();
}
