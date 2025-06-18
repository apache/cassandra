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

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;


@Command(name = "setauditlogrolefilter", description = "Sets the fitler for a series of foles. If the role filter " +
                                                        "already exists, this method will update the audit log filter entry")
public class SetAuditLogRoleFilter extends NodeToolCmd
{
    private static final Pattern ROLE_PATTERN = Pattern.compile("^[a-z_]+$");
    private static final Pattern ACCOUNT_TYPE_PATTERN = Pattern.compile("^[A-Z]+$");

    @Option(title = "role", name = {"--role"}, description = "The role to add to the audit log filters")
    private String role;

    @Option(title = "account_type", name = {"--account-type"}, description = "The type of role being added to the " +
                                                                             "audit log filters\nDefaults to PERSONNEL")
    private String accountType = "PERSONNEL";

    @Option(title = "filter_percent", name = {"--filter-percent"}, description = "The sample rate to filter events " +
                                                                                 "genreated by role\nDefaults to 100.0")
    private double filterPercent = 100.0;

    @Option(title = "refresh", name = "--refresh", description = "Force an immediate refresh of the cache from the "+
                                                                "underlying talbe.")
    private boolean refresh = false;

    @Override
    protected void execute(NodeProbe probe)
    {
        if (StringUtils.isEmpty(role) || !ROLE_PATTERN.matcher(role).matches()) {
            throw new IllegalArgumentException("Role must only contain lowercase letters and underscores");
        }

        if (filterPercent > 100.0 || filterPercent < 0.0) {
            throw new IllegalArgumentException("Filter percent must be between 0.0 and 100.0");
        }

        if (StringUtils.isEmpty(accountType) || !ACCOUNT_TYPE_PATTERN.matcher(accountType).matches()) {
            throw new IllegalArgumentException("Account type must only contain capitals letters");
        }

        probe.setAuditLogRoleFilter(role, accountType, filterPercent, refresh);
    }
}
