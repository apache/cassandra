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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.sequences.CancelCMSReconfiguration;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.Unregister;
import org.apache.cassandra.tcm.transformations.cms.AdvanceCMSReconfiguration;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

public class CMSOperations implements CMSOperationsMBean
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.tcm:type=CMSOperations";

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

    @Override
    public void initializeCMS(List<String> ignoredEndpoints)
    {
        cms.upgradeFromGossip(ignoredEndpoints);
    }

    @Override
    public void resumeReconfigureCms()
    {
        InProgressSequences.finishInProgressSequences(ReconfigureCMS.SequenceKey.instance);
    }


    @Override
    public void reconfigureCMS(int rf)
    {
        cms.reconfigureCMS(ReplicationParams.simpleMeta(rf, ClusterMetadata.current().directory.knownDatacenters()));
    }

    @Override
    public void reconfigureCMS(Map<String, Integer> rf)
    {
        cms.reconfigureCMS(ReplicationParams.ntsMeta(rf));
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
        String members = metadata.fullCMSMembers().stream().sorted().map(Object::toString).collect(Collectors.joining(","));
        info.put("MEMBERS", members);
        info.put("IS_MEMBER", Boolean.toString(cms.isCurrentMember(FBUtilities.getBroadcastAddressAndPort())));
        info.put("SERVICE_STATE", ClusterMetadataService.state(metadata).toString());
        info.put("IS_MIGRATING", Boolean.toString(cms.isMigrating()));
        info.put("EPOCH", Long.toString(metadata.epoch.getEpoch()));
        info.put("LOCAL_PENDING", Integer.toString(cms.log().pendingBufferSize()));
        info.put("COMMITS_PAUSED", Boolean.toString(cms.commitsPaused()));
        info.put("REPLICATION_FACTOR", ReplicationParams.meta(metadata).toString());
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
            cms.commit(new Unregister(nodeId, EnumSet.of(NodeState.LEFT)));
        }
    }
}
