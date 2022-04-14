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

package org.apache.cassandra.service.accord.async;

import java.util.HashMap;
import java.util.Map;

import accord.txn.TxnId;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;

public class AsyncContext
{
    final Map<TxnId, AccordCommand> commands = new HashMap<>();
    final Map<TxnId, AccordCommand> summaries = new HashMap<>();
    final Map<PartitionKey, AccordCommandsForKey> keyCommands = new HashMap<>();

    public AccordCommand command(TxnId txnId)
    {
        return commands.get(txnId);
    }

    public void addCommand(AccordCommand command)
    {
        commands.put(command.txnId(), command);
    }

    public AccordCommand summary(TxnId txnId)
    {
        return summaries.get(txnId);
    }

    public void addSummary(AccordCommand command)
    {
        summaries.put(command.txnId(), command);
    }

    public AccordCommandsForKey commandsForKey(PartitionKey key)
    {
        return keyCommands.get(key);
    }

    void releaseResources(AccordCommandStore commandStore)
    {
        commands.values().forEach(commandStore.commandCache()::release);
        commands.clear();
        keyCommands.values().forEach(commandStore.commandsForKeyCache()::release);
        keyCommands.clear();
    }
}
