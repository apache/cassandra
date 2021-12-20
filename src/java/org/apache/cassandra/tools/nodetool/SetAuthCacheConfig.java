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
import org.apache.cassandra.auth.AuthCacheMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setauthcacheconfig", description = "Set configuration for Auth cache")
public class SetAuthCacheConfig extends NodeToolCmd
{
    @SuppressWarnings("unused")
    @Option(title = "cache-name",
            name = {"--cache-name"},
            description = "Name of Auth cache (required)",
            required = true)
    private String cacheName;

    @SuppressWarnings("unused")
    @Option(title = "validity-period",
            name = {"--validity-period"},
            description = "Validity period in milliseconds")
    private Integer validityPeriod;

    @SuppressWarnings("unused")
    @Option(title = "update-interval",
            name = {"--update-interval"},
            description = "Update interval in milliseconds")
    private Integer updateInterval;

    @SuppressWarnings("unused")
    @Option(title = "max-entries",
            name = {"--max-entries"},
            description = "Max entries")
    private Integer maxEntries;

    @SuppressWarnings("unused")
    @Option(title = "enable-active-update",
            name = {"--enable-active-update"},
            description = "Enable active update")
    private Boolean enableActiveUpdate;

    @SuppressWarnings("unused")
    @Option(title = "disable-active-update",
            name = {"--disable-active-update"},
            description = "Disable active update")
    private Boolean disableActiveUpdate;

    @Override
    public void execute(NodeProbe probe)
    {
        Boolean activeUpdate = getActiveUpdate(enableActiveUpdate, disableActiveUpdate);

        checkArgument(validityPeriod != null || updateInterval != null
                      || maxEntries != null || activeUpdate != null,
                      "At least one optional parameter need to be passed");

        AuthCacheMBean authCacheMBean = probe.getAuthCacheMBean(cacheName);

        if (validityPeriod != null)
        {
            authCacheMBean.setValidity(validityPeriod);
            probe.output().out.println("Changed Validity Period to " + validityPeriod);
        }

        if (updateInterval != null)
        {
            authCacheMBean.setUpdateInterval(updateInterval);
            probe.output().out.println("Changed Update Interval to " + updateInterval);
        }

        if (maxEntries != null)
        {
            authCacheMBean.setMaxEntries(maxEntries);
            probe.output().out.println("Changed Max Entries to " + maxEntries);
        }

        if (activeUpdate != null)
        {
            authCacheMBean.setActiveUpdate(activeUpdate);
            probe.output().out.println("Changed Active Update to " + activeUpdate);
        }
    }

    private Boolean getActiveUpdate(Boolean enableActiveUpdate, Boolean disableActiveUpdate)
    {
        if (enableActiveUpdate == null && disableActiveUpdate == null)
            return null;

        if (enableActiveUpdate != null && disableActiveUpdate != null)
            throw new IllegalArgumentException("enable-active-update and disable-active-update cannot be used together");

        return Boolean.TRUE.equals(enableActiveUpdate) ? Boolean.TRUE : Boolean.FALSE;
    }
}
