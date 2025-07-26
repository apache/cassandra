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

package org.apache.cassandra.tools.nodetool;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import static org.apache.cassandra.tools.nodetool.Help.printTopCommandUsage;

/**
 * The top-level command class for the Cassandra nodetool utility.
 * <p>
 * This class serves as the main entry point for the nodetool command-line interface,
 * which provides administrative and monitoring capabilities for Apache Cassandra clusters.
 * It uses the PicoCLI framework to define and manage a comprehensive set of subcommands
 * that allow users to perform various cluster management operations.
 * </p>
 *
 * <h4>Usage:</h4>
 * <pre>
 * nodetool [global-options] &lt;subcommand&gt; [subcommand-options] [arguments]
 * </pre>
 *
 * <h4>Global Options:</h4>
 * <ul>
 *   <li>{@code --print-port (-pp)}: Operate in 4.0 mode with hosts disambiguated by port number</li>
 * </ul>
 *
 * <p>
 * When executed without any subcommand, this class displays the general usage information
 * and lists all available subcommands. Each subcommand is implemented as a separate class
 * that handles specific administrative tasks.
 * </p>
 *
 * @see Help For displaying command help and usage information
 * @see AbstractCommand Base class for most nodetool subcommands
 * @see JmxConnect For JMX connection management used by subcommands
 */
@Command(name = "nodetool",
         description = "Manage your Cassandra cluster",
         subcommands = { Help.class,
                         AbortBootstrap.class,
                         AccordAdmin.class,
                         AlterTopology.class,
                         Assassinate.class,
                         AutoRepairStatus.class,
                         Bootstrap.class,
                         CIDRFilteringStats.class,
                         CMSAdmin.class,
                         Cleanup.class,
                         ClearSnapshot.class,
                         ClientStats.class,
                         Compact.class,
                         CompactionHistory.class,
                         CompactionStats.class,
                         ConsensusMigrationAdmin.class,
                         DataPaths.class,
                         Decommission.Abort.class,
                         Decommission.class,
                         DescribeCluster.class,
                         DescribeRing.class,
                         DisableAuditLog.class,
                         DisableAutoCompaction.class,
                         DisableBackup.class,
                         DisableBinary.class,
                         DisableFullQueryLog.class,
                         DisableGossip.class,
                         DisableHandoff.class,
                         DisableHintsForDC.class,
                         DisableOldProtocolVersions.class,
                         Drain.class,
                         DropCIDRGroup.class,
                         EnableAuditLog.class,
                         EnableAutoCompaction.class,
                         EnableBackup.class,
                         EnableBinary.class,
                         EnableFullQueryLog.class,
                         EnableGossip.class,
                         EnableHandoff.class,
                         EnableHintsForDC.class,
                         EnableOldProtocolVersions.class,
                         FailureDetectorInfo.class,
                         Flush.class,
                         ForceCompact.class,
                         GarbageCollect.class,
                         GcStats.class,
                         GetAuditLog.class,
                         GetAuthCacheConfig.class,
                         GetAutoRepairConfig.class,
                         GetBatchlogReplayTrottle.class,
                         GetCIDRGroupsOfIP.class,
                         GetColumnIndexSize.class,
                         GetCompactionThreshold.class,
                         GetCompactionThroughput.class,
                         GetConcurrency.class,
                         GetConcurrentCompactors.class,
                         GetConcurrentViewBuilders.class,
                         GetDefaultKeyspaceRF.class,
                         GetEndpoints.class,
                         GetFullQueryLog.class,
                         GetInterDCStreamThroughput.class,
                         GetLoggingLevels.class,
                         GetMaxHintWindow.class,
                         GetSSTables.class,
                         GetSeeds.class,
                         GetSnapshotThrottle.class,
                         GetStreamThroughput.class,
                         GetTimeout.class,
                         GetTraceProbability.class,
                         GossipInfo.class,
                         GuardrailsConfigCommand.GetGuardrailsConfig.class,
                         GuardrailsConfigCommand.SetGuardrailsConfig.class,
                         Import.class,
                         Info.class,
                         InvalidateCIDRPermissionsCache.class,
                         InvalidateCounterCache.class,
                         InvalidateCredentialsCache.class,
                         InvalidateJmxPermissionsCache.class,
                         InvalidateKeyCache.class,
                         InvalidateNetworkPermissionsCache.class,
                         InvalidatePermissionsCache.class,
                         InvalidateRolesCache.class,
                         InvalidateRowCache.class,
                         Join.class,
                         ListCIDRGroups.class,
                         ListPendingHints.class,
                         ListSnapshots.class,
                         Move.Abort.class,
                         Move.class,
                         NetStats.class,
                         PauseHandoff.class,
                         ProfileLoad.class,
                         ProxyHistograms.class,
                         RangeKeySample.class,
                         Rebuild.class,
                         RebuildIndex.class,
                         RecompressSSTables.class,
                         Refresh.class,
                         RefreshSizeEstimates.class,
                         ReloadCIDRGroupsCache.class,
                         ReloadLocalSchema.class,
                         ReloadSeeds.class,
                         ReloadSslCertificates.class,
                         ReloadTriggers.class,
                         RelocateSSTables.class,
                         RemoveNode.Abort.class,
                         RemoveNode.class,
                         Repair.class,
                         RepairAdmin.class,
                         ReplayBatchlog.class,
                         ResetFullQueryLog.class,
                         ResetLocalSchema.class,
                         ResumeHandoff.class,
                         Ring.class,
                         SSTableRepairedSet.class,
                         Scrub.class,
                         SetAuthCacheConfig.class,
                         SetAutoRepairConfig.class,
                         SetBatchlogReplayThrottle.class,
                         SetCacheCapacity.class,
                         SetCacheKeysToSave.class,
                         SetColumnIndexSize.class,
                         SetCompactionThreshold.class,
                         SetCompactionThroughput.class,
                         SetConcurrency.class,
                         SetConcurrentCompactors.class,
                         SetConcurrentViewBuilders.class,
                         SetDefaultKeyspaceRF.class,
                         SetHintedHandoffThrottleInKB.class,
                         SetInterDCStreamThroughput.class,
                         SetLoggingLevel.class,
                         SetMaxHintWindow.class,
                         SetSnapshotThrottle.class,
                         SetStreamThroughput.class,
                         SetTimeout.class,
                         SetTraceProbability.class,
                         Sjk.class,
                         Snapshot.class,
                         Status.class,
                         StatusAutoCompaction.class,
                         StatusBackup.class,
                         StatusBinary.class,
                         StatusGossip.class,
                         StatusHandoff.class,
                         Stop.class,
                         StopDaemon.class,
                         TableHistograms.class,
                         TableStats.class,
                         TopPartitions.class,
                         TpStats.class,
                         TruncateHints.class,
                         UpdateCIDRGroup.class,
                         UpgradeSSTable.class,
                         Verify.class,
                         Version.class,
                         ViewBuildStatus.class })
public class NodetoolCommand implements Runnable
{
    @Spec
    public CommandSpec spec;

    // TODO CASSANDRA-20790 this option is used only in several commands and should not be the global option.
    //  It should be pushed down to specific commands to clean up the global hierarchy, while maintaining backwards compatibility.
    //  Calls such as './nodetool --print-port subcommand', and './nodetool subcommand --print-port' should work as expected.
    @Option(names = { "-pp", "--print-port" }, description = "Operate in 4.0 mode with hosts disambiguated by port number")
    public boolean printPort = false;

    public void run()
    {
        printTopCommandUsage(spec.commandLine(),
                             spec.commandLine().getColorScheme(),
                             spec.commandLine().getOut());
    }
}
