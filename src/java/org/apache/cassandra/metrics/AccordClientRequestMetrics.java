/*
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

package org.apache.cassandra.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class AccordClientRequestMetrics extends ClientRequestMetrics
{
    public final Histogram keySize;

    // During migration back to Paxos it's possible a transaction runs
    // in an Epoch where Accord is no longer accepting transactions
    // and we still run it to completion, but we do skip the read from Cassandra
    // although it would be harmless. This should only occur briefly when coordinators
    // start transactions on the wrong protocol due to temporarily out of data cluster metadata.
    public final Meter migrationSkippedReads;

    // Number of times a key had to be run through PaxosRepair for migration to Accord
    public final Meter paxosKeyMigrations;

    // Number of times a query was rejected by Accord in TxnQuery due to a migration back to Paxos
    public final Meter accordMigrationRejects;

    public AccordClientRequestMetrics(String scope)
    {
        super(scope);

        keySize = Metrics.histogram(factory.createMetricName("KeySizeHistogram"), false);
        migrationSkippedReads = Metrics.meter(factory.createMetricName("MigrationSkippedReads"));
        paxosKeyMigrations = Metrics.meter(factory.createMetricName("PaxosKeyMigrations"));
        accordMigrationRejects = Metrics.meter(factory.createMetricName("AccordMigrationRejects"));
    }

    @Override
    public void release()
    {
        super.release();
        Metrics.remove(factory.createMetricName("KeySizeHistogram"));
        Metrics.remove(factory.createMetricName("MigrationSkippedReads"));
        Metrics.remove(factory.createMetricName("PaxosKeyMigrations"));
        Metrics.remove(factory.createMetricName("AccordMigrationRejects"));
    }
}
