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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.virtual.ClusterMetadataDirectoryTable;
import org.apache.cassandra.db.virtual.ClusterMetadataLogTable;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.membership.EndpointLookup;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.migration.Election;
import org.apache.cassandra.tcm.sequences.CancelCMSReconfiguration;
import org.apache.cassandra.tcm.sequences.DropAccordTable;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.Unregister;
import org.apache.cassandra.tcm.transformations.cms.AdvanceCMSReconfiguration;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration.needsReconfiguration;

public class CMSOperations implements CMSOperationsMBean
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.tcm:type=CMSOperations";
    public static final String MEMBERS = "MEMBERS";
    public static final String NEEDS_RECONFIGURATION = "NEEDS_RECONFIGURATION";
    public static final String IS_MEMBER = "IS_MEMBER";
    public static final String SERVICE_STATE = "SERVICE_STATE";
    public static final String IS_MIGRATING = "IS_MIGRATING";
    public static final String EPOCH = "EPOCH";
    public static final String LOCAL_PENDING = "LOCAL_PENDING";
    public static final String COMMITS_PAUSED = "COMMITS_PAUSED";
    public static final String REPLICATION_FACTOR = "REPLICATION_FACTOR";
    public static final String CMS_ID = "CMS_ID";

    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadataService.class);
    public static CMSOperations instance = new CMSOperations(ClusterMetadataService.instance());

    public static void initJmx()
    {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_OBJECT_NAME);
    }

    private final ClusterMetadataService cms;

    private CMSOperations(ClusterMetadataService cms)
    {
        this.cms = cms;
    }

    // TCM CMS await timeout
    public long getCmsAwaitTimeoutMillis()
    {
        return DatabaseDescriptor.getCmsAwaitTimeout().to(MILLISECONDS);
    }

    public void setCmsAwaitTimeoutMillis(long timeoutInMillis)
    {
        Preconditions.checkState(timeoutInMillis > 0);
        DatabaseDescriptor.setCmsAwaitTimeout(timeoutInMillis);
    }

    // CMS commit timeout with exponential backoff
    public long getCmsCommitTimeoutMillis()
    {
        return DatabaseDescriptor.getCmsCommitTimeout().to(MILLISECONDS);
    }

    public void setCmsCommitTimeoutMillis(long timeoutInMillis)
    {
        Preconditions.checkState(timeoutInMillis > 0);
        DatabaseDescriptor.setCmsCommitTimeout(timeoutInMillis);
    }

    public long getCmsCommitRetryInitialDelayMillis()
    {
        return DatabaseDescriptor.getCmsCommitRetryInitialDelay().to(MILLISECONDS);
    }

    public void setCmsCommitRetryInitialDelayMillis(long delayInMillis)
    {
        Preconditions.checkState(delayInMillis > 0);
        DatabaseDescriptor.setCmsCommitRetryInitialDelay(delayInMillis);
    }

    public long getCmsCommitRetryMaxDelayMillis()
    {
        return DatabaseDescriptor.getCmsCommitRetryMaxDelay().to(MILLISECONDS);
    }

    public void setCmsCommitRetryMaxDelayMillis(long delayInMillis)
    {
        Preconditions.checkState(delayInMillis > 0);
        DatabaseDescriptor.setCmsCommitRetryMaxDelay(delayInMillis);
    }

    @Override
    public String getCmsCommitMemberPreferencePolicy()
    {
        return DatabaseDescriptor.getCmsCommitMemberPreferencePolicy().name();
    }

    @Override
    public void setCmsCommitMemberPreferencePolicy(String policy)
    {
        DatabaseDescriptor.setCmsCommitMemberPreferencePolicy(policy);
        logger.info("Set cms_commit_member_preference_policy to {}", policy);
    }

    @Override
    public void initializeCMS(List<String> ignoredEndpoints)
    {
        cms.upgradeFromGossip(ignoredEndpoints);
    }

    public void abortInitialization(String initiator)
    {
        Election.instance.abortInitialization(initiator);
    }

    @Override
    public void resumeReconfigureCms()
    {
        InProgressSequences.finishInProgressSequences(ReconfigureCMS.SequenceKey.instance);
    }


    @Override
    public void reconfigureCMS(int rf)
    {
        reconfigureCMS(rf, Collections.emptyList());
    }

    @Override
    public void reconfigureCMS(int rf, List<String> ignoredEndpoints)
    {
        ReplicationParams params = ReplicationParams.simpleMeta(rf, ClusterMetadata.current().directory.knownDatacenters());
        guardMinimumCmsSize(params);
        cms.reconfigureCMS(params, ignoredEndpoints);
    }

    @Override
    public void reconfigureCMS(Map<String, Integer> rf)
    {
        reconfigureCMS(rf, Collections.emptyList());
    }

    @Override
    public void reconfigureCMS(Map<String, Integer> rf, List<String> ignoredEndpoints)
    {
        ReplicationParams params = ReplicationParams.ntsMeta(rf);
        guardMinimumCmsSize(params);
        cms.reconfigureCMS(params, ignoredEndpoints);
    }

    /**
     * Enforces the minimum CMS size guardrail against an operator-requested reconfiguration. This is only applied to
     * explicit reconfigure requests (nodetool / JMX), not to the automatic reconfiguration that happens on topology
     * changes, which reuses the already-committed replication params.
     */
    private void guardMinimumCmsSize(ReplicationParams params)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        int totalNodes = metadata.directory.allJoinedEndpoints().size();
        int cmsSize = params.options.values().stream()
                                    .mapToInt(Integer::parseInt)
                                    .sum();
        Guardrails.minimumCmsSize.guard(totalNodes, cmsSize);
    }

    @Override
    public void cancelReconfigureCms()
    {
        cms.commit(CancelCMSReconfiguration.instance);
    }

    @Override
    public Map<String, List<String>> reconfigureCMSStatus()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        ReconfigureCMS sequence = (ReconfigureCMS) metadata.inProgressSequences.get(ReconfigureCMS.SequenceKey.instance);
        if (sequence == null)
            return null;

        AdvanceCMSReconfiguration advance = sequence.next;
        Map<String, List<String>> status = new LinkedHashMap<>(); // to preserve order
        if (advance.activeTransition != null)
            status.put("ACTIVE", Collections.singletonList(metadata.directory.endpoint(advance.activeTransition.nodeId).toString()));

        if (!advance.diff.additions.isEmpty())
            status.put("ADDITIONS", advance.diff.additions.stream()
                                                          .map(metadata.directory::endpoint)
                                                          .map(Object::toString)
                                                          .collect(Collectors.toList()));

        if (!advance.diff.removals.isEmpty())
            status.put("REMOVALS", advance.diff.removals.stream()
                                                        .map(metadata.directory::endpoint)
                                                        .map(Object::toString)
                                                        .collect(Collectors.toList()));

        if (advance.diff.removals.isEmpty() && advance.diff.additions.isEmpty())
            status.put("INCOMPLETE", Collections.singletonList("All operations have finished but metadata keyspace ranges are still locked"));

        return status;
    }

    @Override
    public Map<String, String> describeCMS()
    {
        Map<String, String> info = new HashMap<>();
        ClusterMetadata metadata = ClusterMetadata.current();
        EndpointLookup endpoints = metadata.endpointLookup();
        String members = metadata.fullCMSMemberIds()
                                 .stream()
                                 .sorted()
                                 .map(id ->  String.format("(nodeid=%s,address=%s)", id.id(), endpoints.endpoint(id)))
                                 .collect(Collectors.joining(","));
        info.put(MEMBERS, members);
        info.put(NEEDS_RECONFIGURATION, Boolean.toString(metadata.epoch.isBefore(Epoch.FIRST) || needsReconfiguration(metadata)));
        info.put(IS_MEMBER, Boolean.toString(metadata.isCMSMember()));
        info.put(SERVICE_STATE, ClusterMetadataService.state(metadata).toString());
        info.put(IS_MIGRATING, Boolean.toString(cms.isMigrating()));
        info.put(EPOCH, Long.toString(metadata.epoch.getEpoch()));
        info.put(LOCAL_PENDING, Integer.toString(cms.log().pendingBufferSize()));
        info.put(COMMITS_PAUSED, Boolean.toString(cms.commitsPaused()));
        info.put(REPLICATION_FACTOR, metadata.epoch.isBefore(Epoch.FIRST) ? "" : ReplicationParams.meta(metadata).toString());
        info.put(CMS_ID, Integer.toString(metadata.metadataIdentifier));
        return info;
    }

    @Override
    public void snapshotClusterMetadata()
    {
        logger.info("Triggering cluster metadata snapshot");
        Epoch epoch = cms.triggerSnapshot().epoch;
        logger.info("Cluster metadata snapshot triggered at {}", epoch);
    }

    @Override
    public void unsafeRevertClusterMetadata(long epoch)
    {
        if (!DatabaseDescriptor.getUnsafeTCMMode())
            throw new IllegalStateException("Cluster is not running unsafe TCM mode, can't revert epoch");
        cms.revertToEpoch(Epoch.create(epoch));
    }

    @Override
    public String dumpClusterMetadata(long epoch, long transformToEpoch, String version) throws IOException
    {
        return cms.dumpClusterMetadata(Epoch.create(epoch), Epoch.create(transformToEpoch), Version.valueOf(version));
    }

    @Override
    public String dumpClusterMetadata() throws IOException
    {
        return dumpClusterMetadata(Epoch.EMPTY.getEpoch(),
                                   ClusterMetadata.current().epoch.getEpoch() + 1000,
                                   NodeVersion.CURRENT.serializationVersion().toString());
    }

    @Override
    public void unsafeLoadClusterMetadata(String file) throws IOException
    {
        if (!DatabaseDescriptor.getUnsafeTCMMode())
            throw new IllegalStateException("Cluster is not running unsafe TCM mode, can't load cluster metadata " + file);
        cms.loadClusterMetadata(file);
    }

    @Override
    public void setCommitsPaused(boolean paused)
    {
        if (paused)
            cms.pauseCommits();
        else
            cms.resumeCommits();
    }

    @Override
    public boolean getCommitsPaused()
    {
        return cms.commitsPaused();
    }

    @Override
    public boolean cancelInProgressSequences(String sequenceOwner, String expectedSequenceKind)
    {
        return InProgressSequences.cancelInProgressSequences(sequenceOwner, expectedSequenceKind);
    }

    @Override
    public void unregisterLeftNodes(List<String> nodeIdStrings)
    {
        List<NodeId> nodeIds = nodeIdStrings.stream().map(NodeId::fromString).collect(Collectors.toList());
        ClusterMetadata metadata = ClusterMetadata.current();
        List<NodeId> nonLeftNodes = nodeIds.stream()
                                           .filter(nodeId -> metadata.directory.peerState(nodeId) != NodeState.LEFT)
                                           .collect(Collectors.toList());
        if (!nonLeftNodes.isEmpty())
        {
            StringBuilder message = new StringBuilder();
            for (NodeId nonLeft : nonLeftNodes)
            {
                NodeState nodeState = metadata.directory.peerState(nonLeft);
                message.append("Node ").append(nonLeft.id()).append(" is in state ").append(nodeState);
                switch (nodeState)
                {
                    case REGISTERED:
                    case BOOTSTRAPPING:
                    case BOOT_REPLACING:
                        message.append(" - need to use `nodetool abortbootstrap` instead of unregistering").append('\n');
                        break;
                    case JOINED:
                        message.append(" - use `nodetool decommission` or `nodetool removenode` to remove this node").append('\n');
                        break;
                    case MOVING:
                        message.append(" - wait until move has been completed, then use `nodetool decommission` or `nodetool removenode` to remove this node").append('\n');
                        break;
                    case LEAVING:
                        message.append(" - wait until leave-operation has completed, then retry this command").append('\n');
                        break;
                }
            }
            throw new IllegalStateException("Can't unregister node(s):\n" + message);
        }

        for (NodeId nodeId : nodeIds)
        {
            logger.info("Unregistering " + nodeId);
            cms.commit(new Unregister(nodeId, EnumSet.of(NodeState.LEFT), ClusterMetadataService.instance().placementProvider()));
        }
    }

    public Map<Long, Map<String, String>> dumpDirectory(boolean tokens)
    {
        Map<Long, Map<String, Object>> directory = ClusterMetadataDirectoryTable.directory(tokens);
        return convertToStringValues(directory);
    }

    public Map<Long, Map<String, String>> dumpLog(long startEpoch, long endEpoch)
    {
        Map<Long, Map<String, Object>> log = ClusterMetadataLogTable.log(startEpoch, endEpoch);
        return convertToStringValues(log);
    }

    @Override
    public boolean getLegacyStateListenerSyncLocalUpdates()
    {
        return DatabaseDescriptor.getLegacyStateListenerSyncLocalUpdates();
    }

    @Override
    public void setLegacyStateListenerSyncLocalUpdates(boolean sync)
    {
        DatabaseDescriptor.setLegacyStateListenerSyncLocalUpdates(sync);
    }

    private Map<Long, Map<String, String>> convertToStringValues(Map<Long, Map<String, Object>> log)
    {
        Map<Long, Map<String, String>> res = new LinkedHashMap<>();
        for (Map.Entry<Long, Map<String, Object>> outerEntry : log.entrySet())
        {
            Map<String, String> rowRes = new HashMap<>();
            for (Map.Entry<String, Object> row : outerEntry.getValue().entrySet())
                rowRes.put(row.getKey(), row.getValue().toString());
            res.put(outerEntry.getKey(), rowRes);
        }
        return res;
    }

    @Override
    public void resumeDropAccordTable(String tableId)
    {
        TableId id = TableId.fromString(tableId);
        for (MultiStepOperation.SequenceKey key : ClusterMetadata.current().inProgressSequences.keys())
        {
            if (key instanceof DropAccordTable.TableReference && ((DropAccordTable.TableReference) key).id.equals(id))
            {
                InProgressSequences.finishInProgressSequences(key);
                return;
            }
        }
        throw new IllegalArgumentException("No drop table operation is in progress for table with id " + tableId);
    }
}
