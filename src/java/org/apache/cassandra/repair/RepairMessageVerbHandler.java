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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Handles all repair related message.
 *
 * @since 2.0
 */
public class RepairMessageVerbHandler implements IVerbHandler<RepairMessage>
{
    public void doVerb(MessageIn<RepairMessage> message, int id)
    {
        // TODO add cancel/interrupt message
        RepairJobDesc desc = message.payload.desc;
        switch (message.payload.messageType)
        {
            case VALIDATION_REQUEST:
                ValidationRequest validationRequest = (ValidationRequest) message.payload;
                // trigger read-only compaction
                ColumnFamilyStore store = Keyspace.open(desc.keyspace).getColumnFamilyStore(desc.columnFamily);
                Validator validator = new Validator(desc, message.from, validationRequest.gcBefore);
                CompactionManager.instance.submitValidation(store, validator);
                break;

            case SYNC_REQUEST:
                // forwarded sync request
                SyncRequest request = (SyncRequest) message.payload;
                StreamingRepairTask task = new StreamingRepairTask(desc, request);
                task.run();
                break;

            default:
                ActiveRepairService.instance.handleMessage(message.from, message.payload);
                break;
        }
    }
}
