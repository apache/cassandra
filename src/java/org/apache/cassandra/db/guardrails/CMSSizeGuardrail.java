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

package org.apache.cassandra.db.guardrails;

import java.util.function.ToLongFunction;

import org.apache.cassandra.service.ClientState;

import static java.lang.String.format;

public class CMSSizeGuardrail extends MinThreshold
{
    /**
     * Creates a new minimum threshold guardrail.
     *
     * @param failThreshold   a {@link ClientState}-based provider of the value above which the operation should be aborted.
     */
    public CMSSizeGuardrail(ToLongFunction<ClientState> failThreshold)
    {
        super("minimum_cms_size",
              null,
              state -> -1,
              failThreshold,
              (isWarning, what, value, threshold) ->
              format("The CMS size of %s is below the failure threshold of %s. " +
                     "Reconfigure CMS so its total size (sum of replication factors across all datacenters) is at least %s.",
                     value, threshold, threshold));
        this.throwOnNullClientState = true;
    }

    public void guard(int totalNodes, int cmsSize)
    {
        // A cluster can't host more CMS replicas than it has nodes. If the request already uses every node there's
        // nothing more to enforce (e.g. during bootstrap), and requiring more would brick reconfiguration. Otherwise
        // the request must meet the configured minimum, even when the threshold exceeds the current cluster size, so
        // an operator can't sidestep the floor by setting a high threshold. An operator who genuinely wants a smaller
        // CMS should disable the guardrail. When it is disabled, guard() below is a no-op.
        if (cmsSize >= totalNodes)
            return;

        guard(cmsSize, "CMS", false, null);
    }
}
