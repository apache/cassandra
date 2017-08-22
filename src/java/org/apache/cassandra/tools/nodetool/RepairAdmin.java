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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.repair.consistent.LocalSessionInfo;
import org.apache.cassandra.service.ActiveRepairServiceMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Supports listing and failing incremental repair sessions
 */
@Command(name = "repair_admin", description = "list and fail incremental repair sessions")
public class RepairAdmin extends NodeTool.NodeToolCmd
{
    @Option(title = "list", name = {"-l", "--list"}, description = "list repair sessions (default behavior)")
    private boolean list = false;

    @Option(title = "all", name = {"-a", "--all"}, description = "include completed and failed sessions")
    private boolean all = false;

    @Option(title = "cancel", name = {"-x", "--cancel"}, description = "cancel an incremental repair session")
    private String cancel = null;

    @Option(title = "force", name = {"-f", "--force"}, description = "cancel repair session from a node other than the repair coordinator." +
                                                                     " Attempting to cancel FINALIZED or FAILED sessions is an error.")
    private boolean force = false;

    private static final List<String> header = Lists.newArrayList("id",
                                                                  "state",
                                                                  "last activity",
                                                                  "coordinator",
                                                                  "participants");


    private List<String> sessionValues(Map<String, String> session, int now)
    {
        int updated = Integer.parseInt(session.get(LocalSessionInfo.LAST_UPDATE));
        return Lists.newArrayList(session.get(LocalSessionInfo.SESSION_ID),
                                  session.get(LocalSessionInfo.STATE),
                                  Integer.toString(now - updated) + " (s)",
                                  session.get(LocalSessionInfo.COORDINATOR),
                                  session.get(LocalSessionInfo.PARTICIPANTS));
    }

    private void listSessions(ActiveRepairServiceMBean repairServiceProxy)
    {
        Preconditions.checkArgument(cancel == null);
        Preconditions.checkArgument(!force, "-f/--force only valid for session cancel");
        List<Map<String, String>> sessions = repairServiceProxy.getSessions(all);
        if (sessions.isEmpty())
        {
            System.out.println("no sessions");

        }
        else
        {
            List<List<String>> rows = new ArrayList<>();
            rows.add(header);
            int now = FBUtilities.nowInSeconds();
            for (Map<String, String> session : sessions)
            {
                rows.add(sessionValues(session, now));
            }

            // get max col widths
            int[] widths = new int[header.size()];
            for (List<String> row : rows)
            {
                assert row.size() == widths.length;
                for (int i = 0; i < widths.length; i++)
                {
                    widths[i] = Math.max(widths[i], row.get(i).length());
                }
            }

            List<String> fmts = new ArrayList<>(widths.length);
            for (int i = 0; i < widths.length; i++)
            {
                fmts.add("%-" + Integer.toString(widths[i]) + "s");
            }


            // print
            for (List<String> row : rows)
            {
                List<String> formatted = new ArrayList<>(row.size());
                for (int i = 0; i < widths.length; i++)
                {
                    formatted.add(String.format(fmts.get(i), row.get(i)));
                }
                System.out.println(Joiner.on(" | ").join(formatted));
            }
        }
    }

    private void cancelSession(ActiveRepairServiceMBean repairServiceProxy)
    {
        Preconditions.checkArgument(!list);
        Preconditions.checkArgument(!all, "-a/--all only valid for session list");
        repairServiceProxy.failSession(cancel, force);
    }

    protected void execute(NodeProbe probe)
    {
        if (list && cancel != null)
        {
            throw new RuntimeException("Can either list, or cancel sessions, not both");
        }
        else if (cancel != null)
        {
            cancelSession(probe.getRepairServiceProxy());
        }
        else
        {
            // default
            listSessions(probe.getRepairServiceProxy());
        }
    }
}
