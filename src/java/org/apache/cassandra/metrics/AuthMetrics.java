package org.apache.cassandra.metrics;

import com.codahale.metrics.Meter;

/**
 * Metrics about authentication
 */
public class AuthMetrics
{

    public static final AuthMetrics instance = new AuthMetrics();

    public static void init()
    {
        // no-op, just used to force instance creation
    }

    /** Number and rate of successful logins */
    protected final Meter success;

    /** Number and rate of login failures */
    protected final Meter failure;

    private AuthMetrics()
    {

        success = ClientMetrics.instance.addMeter("AuthSuccess");
        failure = ClientMetrics.instance.addMeter("AuthFailure");
    }

    public void markSuccess()
    {
        success.mark();
    }

    public void markFailure()
    {
        failure.mark();
    }
}
