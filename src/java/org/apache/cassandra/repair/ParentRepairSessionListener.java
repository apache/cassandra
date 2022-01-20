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
package org.apache.cassandra.repair;

import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.service.ActiveRepairService;

public interface ParentRepairSessionListener
{
    ParentRepairSessionListener instance = CassandraRelevantProperties.REPAIR_PARENT_SESSION_LISTENER.isPresent()
                                      ? make(CassandraRelevantProperties.REPAIR_PARENT_SESSION_LISTENER.getString())
                                      : new NoopParentRepairSessionListener();

    static ParentRepairSessionListener make(String customImpl)
    {
        try
        {
            return (ParentRepairSessionListener) Class.forName(customImpl).newInstance();
        }
        catch (Throwable ex)
        {
            throw new IllegalStateException("Unknown parent repair session listener: " + customImpl);
        }
    }

    /**
     * Call when parent repair session is registered
     */
    void onRegistered(UUID sessionId, ActiveRepairService.ParentRepairSession session);

    /**
     * Call when parent repair session is removed
     */
    void onRemoved(UUID sessionId, ActiveRepairService.ParentRepairSession session);

    /**
     * Call when validation task started for given repair session
     */
    void onValidation(RepairJobDesc desc, Future validationTask);

    /**
     * Call when sync task started for given repair session
     */
    void onSync(RepairJobDesc desc, Future syncTask);

    static class NoopParentRepairSessionListener implements ParentRepairSessionListener
    {
        @Override
        public void onRegistered(UUID sessionId, ActiveRepairService.ParentRepairSession session)
        {
        }

        @Override
        public void onRemoved(UUID sessionId, ActiveRepairService.ParentRepairSession session)
        {
        }

        @Override
        public void onValidation(RepairJobDesc desc, Future validationTask)
        {
        }

        @Override
        public void onSync(RepairJobDesc desc, Future syncTask)
        {
        }
    }
}
