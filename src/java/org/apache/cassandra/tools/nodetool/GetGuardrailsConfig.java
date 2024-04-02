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

import java.util.Collections;
import java.util.Set;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.db.guardrails.GuardrailsMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "getguardrailsconfig", description = "Print current guardrails configurations")
public class GetGuardrailsConfig extends NodeTool.NodeToolCmd
{
    @Option(title = "show_full_config",
    name = {"-f", "--full"},
    description = "Show full guardrails configuration (including disabled)")
    private boolean showFullConfig = false;

    private final StringBuilder sb = new StringBuilder();

    @Override
    public void execute(NodeProbe probe)
    {
        GuardrailsMBean mbean = probe.getGuardrailsMBean();
        sb.append("Guardrails Configuration:\n");
        sb.append("guardrails applied on supuerusers\n");
        sb.append("\tenabled: ").append(mbean.getGuardrailsOnSuperuserEnabled()).append('\n');
        printConfigHelper(mbean);
        probe.output().out.println(sb);
    }

    private void printConfigHelper(GuardrailsMBean mbean)
    {
        // See Guardrails.java for all available guradrails
        warnFailMaxThresholdStringHelper("total number of user keyspaces",
                                         mbean.getKeyspacesWarnThreshold(),
                                         mbean.getKeyspacesFailThreshold());
        warnFailMaxThresholdStringHelper("total number of tables on user keyspaces",
                                         mbean.getTablesWarnThreshold(),
                                         mbean.getTablesFailThreshold());
        warnFailMaxThresholdStringHelper("number of columns per table",
                                         mbean.getColumnsPerTableWarnThreshold(),
                                         mbean.getColumnsPerTableFailThreshold());
        warnFailMaxThresholdStringHelper("number of secondary indexes per table",
                                         mbean.getSecondaryIndexesPerTableWarnThreshold(),
                                         mbean.getSecondaryIndexesPerTableFailThreshold());
        enableStringHelper("ability to create secondary indexes",
                           mbean.getSecondaryIndexesEnabled());
        warnFailMaxThresholdStringHelper("number of materialized views per table",
                                         mbean.getMaterializedViewsPerTableWarnThreshold(),
                                         mbean.getMaterializedViewsPerTableFailThreshold());
        warnIgnoredDisallowedValuesStringHelper("usage of certain table properties",
                                                mbean.getTablePropertiesWarned(),
                                                mbean.getTablePropertiesIgnored(),
                                                mbean.getTablePropertiesDisallowed());
        enableStringHelper("ability to use user-provided timestamps",
                           mbean.getUserTimestampsEnabled());
        enableStringHelper("ability to use GROUP BY",
                           mbean.getGroupByEnabled());
        enableStringHelper("ability to use DROP and TRUNCATE TABLE",
                           mbean.getDropTruncateTableEnabled());
        enableStringHelper("ability to do bulk load",
                           mbean.getBulkLoadEnabled());
        enableStringHelper("ability to execute DDL statements",
                           mbean.getDDLEnabled());
        enableStringHelper("ability to execute DCL statements",
                           mbean.getDCLEnabled());
        enableStringHelper("ability to turn off compression",
                           mbean.getUncompressedTablesEnabled());
        enableStringHelper("ability to create new COMPACT STORAGE tables",
                           mbean.getCompactTablesEnabled());
        warnFailMaxThresholdStringHelper("number of elements returned within page",
                                         mbean.getPageSizeWarnThreshold(),
                                         mbean.getPageSizeFailThreshold());
        warnFailMaxThresholdStringHelper("number of partition keys in the IN clause",
                                         mbean.getPartitionKeysInSelectWarnThreshold(),
                                         mbean.getPartitionKeysInSelectFailThreshold());
        enableStringHelper("ability on operate lists that require read before write",
                           mbean.getReadBeforeWriteListOperationsEnabled());
        enableStringHelper("ability to execute statement with ALLOW FILTERING",
                           mbean.getAllowFilteringEnabled());
        warnFailMaxThresholdStringHelper("number of restrictions created by a cartesian product of a CQL's IN query",
                                         mbean.getInSelectCartesianProductWarnThreshold(),
                                         mbean.getInSelectCartesianProductFailThreshold());
        warnIgnoredDisallowedValuesStringHelper("usage on read consistency levels",
                                                mbean.getReadConsistencyLevelsWarned(),
                                                Collections.emptySet(),
                                                mbean.getReadConsistencyLevelsDisallowed());
        warnIgnoredDisallowedValuesStringHelper("usage on write consistency levels",
                                                mbean.getWriteConsistencyLevelsWarned(),
                                                Collections.emptySet(),
                                                mbean.getWriteConsistencyLevelsDisallowed());
        warnFailMaxThresholdStringHelper("size of a collection",
                                         mbean.getCollectionSizeWarnThreshold(),
                                         mbean.getCollectionSizeFailThreshold());
        warnFailMaxThresholdStringHelper("number of items of a collection",
                                         mbean.getItemsPerCollectionWarnThreshold(),
                                         mbean.getItemsPerCollectionFailThreshold());
        warnFailMaxThresholdStringHelper("number of fields on each UDT",
                                         mbean.getFieldsPerUDTWarnThreshold(),
                                         mbean.getFieldsPerUDTFailThreshold());
        warnFailMaxPercentageStringHelper("data disk usage percentage on the local node, used by a periodic task to " +
                                          "calculate and propagate that status",
                                          mbean.getDataDiskUsagePercentageWarnThreshold(),
                                          mbean.getDataDiskUsagePercentageFailThreshold());
        warnFailMinThresholdStringHelper("number of minimum replication factor",
                                         mbean.getMinimumReplicationFactorWarnThreshold(),
                                         mbean.getMinimumReplicationFactorFailThreshold());
    }

    private void warnFailMaxThresholdStringHelper(String title, long warn, long fail)
    {
        if (!showFullConfig && isGuardrailDisabled(warn) && isGuardrailDisabled(fail)) {
            // hide this config if disabled
            return;
        }
        sb.append(title).append('\n');
        sb.append(String.format("\twarning threshold(maximum): %d\n\tfailing threashold(maximum): %d\n", warn, fail));
    }

    private void warnFailMaxThresholdStringHelper(String title, String warn, String fail)
    {
        if (!showFullConfig && isGuardrailDisabled(warn) && isGuardrailDisabled(fail)) {
            // hide this config if disabled
            return;
        }
        sb.append(title).append('\n');
        sb.append(String.format("\twarning threshold(maximum): %s\n\tfailing threashold(maximum): %s\n", warn, fail));
    }
    private void warnFailMinThresholdStringHelper(String title, long warn, long fail)
    {
        if (!showFullConfig && isGuardrailDisabled(warn) && isGuardrailDisabled(fail)) {
            // hide this config if disabled
            return;
        }
        sb.append(title).append('\n');
        sb.append(String.format("\twarning threshold(minimum): %d\n\tfailing threashold(minimum): %d\n", warn, fail));
    }

    private void warnFailMaxPercentageStringHelper(String title, long warn, long fail)
    {
        if (!showFullConfig && isGuardrailDisabled(warn) && isGuardrailDisabled(fail)) {
            // hide this config if disabled
            return;
        }
        sb.append(title).append('\n');
        sb.append(String.format("\twarning threshold(max percentage): %d%%\n\tfailing threashold(max percentage): %d%%\n", warn, fail));
    }
    private void enableStringHelper(String title, boolean enabled)
    {
        if (!showFullConfig && enabled) {
            // hide this config if guardrail disabled (by default all flags are enable=true)
            return;
        }
        sb.append(title).append('\n');
        sb.append(String.format("\tenabled: %s\n", enabled));
    }
    private <T> void warnIgnoredDisallowedValuesStringHelper
    (String title, Set<T> warn, Set<T> ignored, Set<T> disallowed) {
        if (!showFullConfig && isGuardrailDisabled(warn) && isGuardrailDisabled(ignored) && isGuardrailDisabled(disallowed)) {
            // hide this config if disabled
            return;
        }
        sb.append(title).append('\n');
        sb.append("\twarning values: ");
        addSetStringHelper(warn);
        sb.append("\tignored values: ");
        addSetStringHelper(ignored);
        sb.append("\tdisallowed values: ");
        addSetStringHelper(disallowed);
    }

    private <T> void addSetStringHelper(Set<T> s) {
        if (s == null || s.isEmpty()) {
            sb.append("null").append('\n');
            return;
        }
        boolean isFirst = true;
        for (T v : s) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(',');
            }
            sb.append(v);
        }
        sb.append('\n');
    }

    private Boolean isGuardrailDisabled(long threshold) {
        return threshold <= 0;
    }

    private Boolean isGuardrailDisabled(String threshold) {
        return threshold == null || threshold.equals("null");
    }

    private <T> Boolean isGuardrailDisabled(Set<T> props) {
        return props == null || props.isEmpty();
    }
}
