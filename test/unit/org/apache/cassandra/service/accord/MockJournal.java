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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import accord.local.SerializerSupport;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.ApplyThenWaitUntilApplied;
import accord.messages.BeginRecovery;
import accord.messages.Commit;
import accord.messages.Message;
import accord.messages.MessageType;
import accord.messages.PreAccept;
import accord.messages.Propagate;
import accord.primitives.Ballot;
import accord.primitives.TxnId;
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.service.accord.AccordJournal.Key;
import org.apache.cassandra.service.accord.AccordJournal.Type;

import static accord.messages.MessageType.ACCEPT_REQ;
import static accord.messages.MessageType.APPLY_MAXIMAL_REQ;
import static accord.messages.MessageType.APPLY_MINIMAL_REQ;
import static accord.messages.MessageType.APPLY_THEN_WAIT_UNTIL_APPLIED_REQ;
import static accord.messages.MessageType.BEGIN_RECOVER_REQ;
import static accord.messages.MessageType.COMMIT_MAXIMAL_REQ;
import static accord.messages.MessageType.COMMIT_SLOW_PATH_REQ;
import static accord.messages.MessageType.PRE_ACCEPT_REQ;
import static accord.messages.MessageType.PROPAGATE_APPLY_MSG;
import static accord.messages.MessageType.PROPAGATE_OTHER_MSG;
import static accord.messages.MessageType.PROPAGATE_PRE_ACCEPT_MSG;
import static accord.messages.MessageType.PROPAGATE_STABLE_MSG;
import static accord.messages.MessageType.STABLE_FAST_PATH_REQ;
import static accord.messages.MessageType.STABLE_MAXIMAL_REQ;
import static accord.messages.MessageType.STABLE_SLOW_PATH_REQ;

public class MockJournal implements IJournal
{
    private static final Runnable NO_OP = () -> {};

    private final Map<Key, Message> writes = new HashMap<>();
    private final Map<Key, List<SavedCommand.LoadedDiff>> commands = new HashMap<>();
    @Override
    public SerializerSupport.MessageProvider makeMessageProvider(TxnId txnId)
    {
        return new SerializerSupport.MessageProvider()
        {
            @Override
            public TxnId txnId()
            {
                return txnId;
            }

            @Override
            public Set<MessageType> test(Set<MessageType> messages)
            {
                Set<Key> keys = new ObjectHashSet<>(messages.size() + 1, 0.9f);
                for (MessageType message : messages)
                    for (Type synonymousType : Type.synonymousTypesFromMessageType(message))
                        keys.add(new Key(txnId, synonymousType));
                Set<Key> presentKeys = Sets.intersection(writes.keySet(), keys);
                Set<MessageType> presentMessages = new ObjectHashSet<>(presentKeys.size() + 1, 0.9f);
                for (Key key : presentKeys)
                    presentMessages.add(key.type.outgoingType);
                return presentMessages;
            }

            @Override
            public Set<MessageType> all()
            {
                Set<Type> types = EnumSet.allOf(Type.class);
                Set<Key> keys = new ObjectHashSet<>(types.size() + 1, 0.9f);
                for (Type type : types)
                    keys.add(new Key(txnId, type));
                Set<Key> presentKeys = Sets.intersection(writes.keySet(), keys);
                Set<MessageType> presentMessages = new ObjectHashSet<>(presentKeys.size() + 1, 0.9f);
                for (Key key : presentKeys)
                    presentMessages.add(key.type.outgoingType);
                return presentMessages;
            }

            private <T extends Message> T get(Key key)
            {
                return (T) writes.get(key);
            }

            private <T extends Message> T get(MessageType messageType)
            {
                for (Type type : Type.synonymousTypesFromMessageType(messageType))
                {
                    T value = get(new Key(txnId, type));
                    if (value != null) return value;
                }
                return null;
            }

            @Override
            public PreAccept preAccept()
            {
                return get(PRE_ACCEPT_REQ);
            }

            @Override
            public BeginRecovery beginRecover()
            {
                return get(BEGIN_RECOVER_REQ);
            }

            @Override
            public Propagate propagatePreAccept()
            {
                return get(PROPAGATE_PRE_ACCEPT_MSG);
            }

            @Override
            public Accept accept(Ballot ballot)
            {
                return get(ACCEPT_REQ);
            }

            @Override
            public Commit commitSlowPath()
            {
                return get(COMMIT_SLOW_PATH_REQ);
            }

            @Override
            public Commit commitMaximal()
            {
                return get(COMMIT_MAXIMAL_REQ);
            }

            @Override
            public Commit stableFastPath()
            {
                return get(STABLE_FAST_PATH_REQ);
            }

            @Override
            public Commit stableSlowPath()
            {
                return get(STABLE_SLOW_PATH_REQ);
            }

            @Override
            public Commit stableMaximal()
            {
                return get(STABLE_MAXIMAL_REQ);
            }

            @Override
            public Propagate propagateStable()
            {
                return get(PROPAGATE_STABLE_MSG);
            }

            @Override
            public Apply applyMinimal()
            {
                return get(APPLY_MINIMAL_REQ);
            }

            @Override
            public Apply applyMaximal()
            {
                return get(APPLY_MAXIMAL_REQ);
            }

            @Override
            public Propagate propagateApply()
            {
                return get(PROPAGATE_APPLY_MSG);
            }

            @Override
            public Propagate propagateOther()
            {
                return get(PROPAGATE_OTHER_MSG);
            }

            @Override
            public ApplyThenWaitUntilApplied applyThenWaitUntilApplied()
            {
                return get(APPLY_THEN_WAIT_UNTIL_APPLIED_REQ);
            }
        };
    }

    @Override
    public List<SavedCommand.LoadedDiff> loadAll(int commandStoreId, TxnId txnId)
    {
        Type type = Type.SAVED_COMMAND;
        Key key = new Key(txnId, type, commandStoreId);
        List<SavedCommand.LoadedDiff> saved = commands.get(key);
        if (saved == null)
            return null;
        return new ArrayList<>(saved);
    }

    @Override
    public void appendMessageBlocking(Message message)
    {
        Type type = Type.fromMessageType(message.type());
        Key key = new Key(type.txnId(message), type);
        writes.put(key, message);
    }

    @Override
    public boolean appendCommand(int commandStoreId, TxnId txnId, SavedCommand.SavedDiff command, Runnable runnable)
    {
        Type type = Type.SAVED_COMMAND;
        Key key = new Key(txnId, type, commandStoreId);
        commands.computeIfAbsent(key, (ignore_) -> new ArrayList<>())
                .add(new SavedCommand.LoadedDiff(command.txnId,
                                                 command.executeAt,
                                                 command.saveStatus,
                                                 command.durability,
                                                 command.acceptedOrCommitted,
                                                 command.promised,
                                                 command.route,
                                                 command.partialTxn,
                                                 command.partialDeps,
                                                 (i1, i2) -> command.waitingOn,
                                                 command.writes));
        runnable.run();
        return false;
    }
}
