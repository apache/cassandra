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

package org.apache.cassandra.service.accord;

import java.util.List;

import accord.local.SerializerSupport;
import accord.messages.Message;
import accord.primitives.TxnId;

public interface IJournal
{
    SerializerSupport.MessageProvider makeMessageProvider(TxnId txnId);
    List<SavedCommand.LoadedDiff> loadAll(int commandStoreId, TxnId txnId);
    void appendMessageBlocking(Message message);

    /**
     * Append outcomes to the log. Returns a runnable; when this runnable returns,
     * all commands are guaranteed to be flushed.
     */
    boolean appendCommand(int commandStoreId, TxnId txnId, SavedCommand.SavedDiff command, Runnable callback);
}