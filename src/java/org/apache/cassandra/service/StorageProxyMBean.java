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
package org.apache.cassandra.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.ConsistencyLevel;

public interface StorageProxyMBean
{
    public long getTotalHints();
    public boolean getHintedHandoffEnabled();
    public void setHintedHandoffEnabled(boolean b);
    public void enableHintsForDC(String dc);
    public void disableHintsForDC(String dc);
    public Set<String> getHintedHandoffDisabledDCs();
    public int getMaxHintWindow();
    public void setMaxHintWindow(int ms);
    public int getMaxHintsInProgress();
    public void setMaxHintsInProgress(int qs);
    public int getHintsInProgress();

    public Long getRpcTimeout();
    public void setRpcTimeout(Long timeoutInMillis);
    public Long getReadRpcTimeout();
    public void setReadRpcTimeout(Long timeoutInMillis);
    public Long getWriteRpcTimeout();
    public void setWriteRpcTimeout(Long timeoutInMillis);
    public Long getCounterWriteRpcTimeout();
    public void setCounterWriteRpcTimeout(Long timeoutInMillis);
    public Long getCasContentionTimeout();
    public void setCasContentionTimeout(Long timeoutInMillis);
    public Long getRangeRpcTimeout();
    public void setRangeRpcTimeout(Long timeoutInMillis);
    public Long getTruncateRpcTimeout();
    public void setTruncateRpcTimeout(Long timeoutInMillis);

    public void setNativeTransportMaxConcurrentConnections(Long nativeTransportMaxConcurrentConnections);
    public Long getNativeTransportMaxConcurrentConnections();

    public void reloadTriggerClasses();

    public long getReadRepairAttempted();
    public long getReadRepairRepairedBlocking();
    public long getReadRepairRepairedBackground();

    public int getOtcBacklogExpirationInterval();
    public void setOtcBacklogExpirationInterval(int intervalInMillis);

    /** Returns each live node's schema version */
    public Map<String, List<String>> getSchemaVersions();

    public int getNumberOfTables();

    public String getIdealConsistencyLevel();
    public String setIdealConsistencyLevel(String cl);

    /**
     * Start the fully query logger.
     * @param path Path where the full query log will be stored. If null cassandra.yaml value is used.
     * @param rollCycle How often to create a new file for query data (MINUTELY, DAILY, HOURLY)
     * @param blocking Whether threads submitting queries to the query log should block if they can't be drained to the filesystem or alternatively drops samples and log
     * @param maxQueueWeight How many bytes of query data to queue before blocking or dropping samples
     * @param maxLogSize How many bytes of log data to store before dropping segments. Might not be respected if a log file hasn't rolled so it can be deleted.
     */
    public void configureFullQueryLogger(String path, String rollCycle, boolean blocking, int maxQueueWeight, long maxLogSize);

    /**
     * Disable the full query logger if it is enabled.
     * Also delete any generated files in the last used full query log path as well as the one configure in cassandra.yaml
     */
    public void resetFullQueryLogger();

    /**
     * Stop logging queries but leave any generated files on disk.
     */
    public void stopFullQueryLogger();
}
