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
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.List;

public interface DynamicEndpointSnitchMBean 
{
    public Map<String, Double> getScoresWithPort();
    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0")
    public Map<InetAddress, Double> getScores();
    public int getUpdateInterval();
    public int getResetInterval();
    public double getBadnessThreshold();
    public String getSubsnitchClassName();
    public List<Double> dumpTimings(String hostname) throws UnknownHostException;

    /**
     * Setting a Severity allows operators to inject preference information into the Dynamic Snitch
     * replica selection.
     *
     * When choosing which replicas to participate in a read request, the DSnitch sorts replicas
     * by response latency, and selects the fastest replicas.  Latencies are normalized to a score
     * from 0 to 1,  with lower scores being faster.
     *
     * The Severity injected here will be added to the normalized score.
     *
     * Thus, adding a Severity greater than 1 will mean the replica will never be contacted
     * (unless needed for ALL or if it is added later for rapid read protection).
     *
     * Conversely, adding a negative Severity means the replica will *always* be contacted.
     *
     * (The "Severity" term is historical and dates to when this was used to represent how
     * badly background tasks like compaction were affecting a replica's performance.
     * See CASSANDRA-3722 for when this was introduced and CASSANDRA-11738 for why it was removed.)
     */
    public void setSeverity(double severity);

    /**
     * @return the current manually injected Severity.
     */
    public double getSeverity();
}
