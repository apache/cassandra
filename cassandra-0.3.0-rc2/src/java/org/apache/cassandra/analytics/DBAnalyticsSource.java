/**
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

package org.apache.cassandra.analytics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class sets up the analytics package to report metrics into
 * Ganglia for the various DB operations such as: reads per second,
 * average read latency, writes per second, average write latency.
 *
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com ) & Karthik Ranganathan ( kranganathan@facebook.com )
 */
public class DBAnalyticsSource implements IAnalyticsSource
{
    private static final String METRIC_READ_OPS = "Read Operations";
    private static final String RECORD_READ_OPS = "ReadOperationsRecord";
    private static final String TAG_READOPS = "ReadOperationsTag";
    private static final String TAG_READ_OPS = "ReadOperationsTagValue";

    private static final String METRIC_READ_AVG = "Average Read Latency";
    private static final String RECORD_READ_AVG = "ReadLatencyRecord";
    private static final String TAG_READAVG = "AverageReadLatencyTag";
    private static final String TAG_READ_AVG = "ReadLatencyTagValue";

    private static final String METRIC_WRITE_OPS = "Write Operations";
    private static final String RECORD_WRITE_OPS = "WriteOperationsRecord";
    private static final String TAG_WRITEOPS = "WriteOperationsTag";
    private static final String TAG_WRITE_OPS = "WriteOperationsTagValue";

    private static final String METRIC_WRITE_AVG = "Average Write Latency";
    private static final String RECORD_WRITE_AVG = "WriteLatencyRecord";
    private static final String TAG_WRITEAVG = "AverageWriteLatencyTag";
    private static final String TAG_WRITE_AVG = "WriteLatencyTagValue";

    /* keep track of the number of read operations */
    private AtomicInteger readOperations_ = new AtomicInteger(0);

    /* keep track of the number of read latencies */
    private AtomicLong readLatencies_ = new AtomicLong(0);

    /* keep track of the number of write operations */
    private AtomicInteger writeOperations_ = new AtomicInteger(0);

    /* keep track of the number of write latencies */
    private AtomicLong writeLatencies_ = new AtomicLong(0);

    /**
     * Create all the required records we intend to display, and
     * register with the AnalyticsContext.
     */
    public DBAnalyticsSource()
    {
        /* register with the AnalyticsContext */
        AnalyticsContext.instance().registerUpdater(this);
        /* set the units for the metric type */
        AnalyticsContext.instance().setAttribute("units." + METRIC_READ_OPS, "r/s");
        /* create the record */
        AnalyticsContext.instance().createRecord(RECORD_READ_OPS);

        /* set the units for the metric type */
        AnalyticsContext.instance().setAttribute("units." + METRIC_READ_AVG, "ms");
        /* create the record */
        AnalyticsContext.instance().createRecord(RECORD_READ_AVG);

        /* set the units for the metric type */
        AnalyticsContext.instance().setAttribute("units." + METRIC_WRITE_OPS, "w/s");
        /* create the record */
        AnalyticsContext.instance().createRecord(RECORD_WRITE_OPS);

        /* set the units for the metric type */
        AnalyticsContext.instance().setAttribute("units." + METRIC_WRITE_AVG, "ms");
        /* create the record */
        AnalyticsContext.instance().createRecord(RECORD_WRITE_AVG);
    }

    /**
     * Update each of the records with the relevant data
     *
     * @param context the reference to the context which has called this callback
     */
    public void doUpdates(AnalyticsContext context)
    {
        // update the read operations record
        MetricsRecord readUsageRecord = context.getMetricsRecord(RECORD_READ_OPS);
        int period = context.getPeriod();

        if(readUsageRecord != null)
        {
            if ( readOperations_.get() > 0 )
            {
                readUsageRecord.setTag(TAG_READOPS, TAG_READ_OPS);
                readUsageRecord.setMetric(METRIC_READ_OPS, readOperations_.get() / period);
                readUsageRecord.update();
            }
        }

        // update the read latency record
        MetricsRecord readLatencyRecord = context.getMetricsRecord(RECORD_READ_AVG);
        if(readLatencyRecord != null)
        {
            if ( readOperations_.get() > 0 )
            {
                readLatencyRecord.setTag(TAG_READAVG, TAG_READ_AVG);
                readLatencyRecord.setMetric(METRIC_READ_AVG, readLatencies_.get() / readOperations_.get() );
                readLatencyRecord.update();
            }
        }

        // update the write operations record
        MetricsRecord writeUsageRecord = context.getMetricsRecord(RECORD_WRITE_OPS);
        if(writeUsageRecord != null)
        {
            if ( writeOperations_.get() > 0 )
            {
                writeUsageRecord.setTag(TAG_WRITEOPS, TAG_WRITE_OPS);
                writeUsageRecord.setMetric(METRIC_WRITE_OPS, writeOperations_.get() / period);
                writeUsageRecord.update();
            }
        }

        // update the write latency record
        MetricsRecord writeLatencyRecord = context.getMetricsRecord(RECORD_WRITE_AVG);
        if(writeLatencyRecord != null)
        {
            if ( writeOperations_.get() > 0 )
            {
                writeLatencyRecord.setTag(TAG_WRITEAVG, TAG_WRITE_AVG);
                writeLatencyRecord.setMetric(METRIC_WRITE_AVG, writeLatencies_.get() / writeOperations_.get() );
                writeLatencyRecord.update();
            }
        }

        clear();
    }

    /**
     * Reset all the metric records
     */
    private void clear()
    {
        readOperations_.set(0);
        readLatencies_.set(0);
        writeOperations_.set(0);
        writeLatencies_.set(0);
    }

    /**
     * Update the read statistics.
     */
    public void updateReadStatistics(long latency)
    {
        readOperations_.incrementAndGet();
        readLatencies_.addAndGet(latency);
    }

    /**
     * Update the write statistics.
     */
    public void updateWriteStatistics(long latency)
    {
        writeOperations_.incrementAndGet();
        writeLatencies_.addAndGet(latency);
    }
}
