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
    public int getMaxHintsSizePerHostInMiB();
    public void setMaxHintsSizePerHostInMiB(int value);
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
    public long getReadRepairRepairTimedOut();

    /** @deprecated See CASSANDRA-15066 */
    @Deprecated(since = "4.0")
    public int getOtcBacklogExpirationInterval();

    public void loadPartitionDenylist();
    public int getPartitionDenylistLoadAttempts();
    public int getPartitionDenylistLoadSuccesses();
    public void setEnablePartitionDenylist(boolean enabled);
    public void setEnableDenylistWrites(boolean enabled);
    public void setEnableDenylistReads(boolean enabled);
    public void setEnableDenylistRangeReads(boolean enabled);
    public boolean denylistKey(String keyspace, String table, String partitionKeyAsString);
    public boolean removeDenylistKey(String keyspace, String table, String partitionKeyAsString);
    public void setDenylistMaxKeysPerTable(int value);
    public void setDenylistMaxKeysTotal(int value);
    public boolean isKeyDenylisted(String keyspace, String table, String partitionKeyAsString);

    /** @deprecated See CASSANDRA-15066 */
    @Deprecated(since = "4.0")
    public void setOtcBacklogExpirationInterval(int intervalInMillis);

    /**
     * Returns each live node's schema version.
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0") public Map<String, List<String>> getSchemaVersions();
    public Map<String, List<String>> getSchemaVersionsWithPort();

    public int getNumberOfTables();

    public String getIdealConsistencyLevel();
    public String setIdealConsistencyLevel(String cl);

    public void logBlockingReadRepairAttemptsForNSeconds(int seconds);
    public boolean isLoggingReadRepairs();

    /**
     * Tracking and reporting of variances in the repaired data set across replicas at read time
     */
    void enableRepairedDataTrackingForRangeReads();
    void disableRepairedDataTrackingForRangeReads();
    boolean getRepairedDataTrackingEnabledForRangeReads();

    void enableRepairedDataTrackingForPartitionReads();
    void disableRepairedDataTrackingForPartitionReads();
    boolean getRepairedDataTrackingEnabledForPartitionReads();

    void enableReportingUnconfirmedRepairedDataMismatches();
    void disableReportingUnconfirmedRepairedDataMismatches();
    boolean getReportingUnconfirmedRepairedDataMismatchesEnabled();

    void enableSnapshotOnRepairedDataMismatch();
    void disableSnapshotOnRepairedDataMismatch();
    boolean getSnapshotOnRepairedDataMismatchEnabled();

    void enableSnapshotOnDuplicateRowDetection();
    void disableSnapshotOnDuplicateRowDetection();
    boolean getSnapshotOnDuplicateRowDetectionEnabled();

    boolean getCheckForDuplicateRowsDuringReads();
    void enableCheckForDuplicateRowsDuringReads();
    void disableCheckForDuplicateRowsDuringReads();
    boolean getCheckForDuplicateRowsDuringCompaction();
    void enableCheckForDuplicateRowsDuringCompaction();
    void disableCheckForDuplicateRowsDuringCompaction();

    void setPaxosVariant(String variant);
    String getPaxosVariant();

    boolean getUseStatementsEnabled();
    void setUseStatementsEnabled(boolean enabled);

    void setPaxosContentionStrategy(String variant);
    String getPaxosContentionStrategy();

    void setPaxosCoordinatorLockingDisabled(boolean disabled);
    boolean getPaxosCoordinatorLockingDisabled();

    public boolean getDumpHeapOnUncaughtException();
    public void setDumpHeapOnUncaughtException(boolean enabled);

    boolean getSStableReadRatePersistenceEnabled();
    void setSStableReadRatePersistenceEnabled(boolean enabled);

    boolean getClientRequestSizeMetricsEnabled();
    void setClientRequestSizeMetricsEnabled(boolean enabled);
}
