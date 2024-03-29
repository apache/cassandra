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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import accord.local.SerializerSupport;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.BeginRecovery;
import accord.messages.Commit;
import accord.messages.Message;
import accord.messages.MessageType;
import accord.messages.PreAccept;
import accord.messages.Propagate;
import accord.primitives.Ballot;
import accord.primitives.TxnId;
import org.agrona.collections.ObjectHashSet;

import static accord.messages.MessageType.ACCEPT_REQ;
import static accord.messages.MessageType.APPLY_MAXIMAL_REQ;
import static accord.messages.MessageType.APPLY_MINIMAL_REQ;
import static accord.messages.MessageType.BEGIN_RECOVER_REQ;
import static accord.messages.MessageType.COMMIT_MAXIMAL_REQ;
import static accord.messages.MessageType.COMMIT_SLOW_PATH_REQ;
import static accord.messages.MessageType.PRE_ACCEPT_REQ;
import static accord.messages.MessageType.PROPAGATE_APPLY_MSG;
import static accord.messages.MessageType.PROPAGATE_PRE_ACCEPT_MSG;
import static accord.messages.MessageType.PROPAGATE_STABLE_MSG;
import static accord.messages.MessageType.STABLE_FAST_PATH_REQ;
import static accord.messages.MessageType.STABLE_MAXIMAL_REQ;

public class MockJournal implements IJournal
{
    private final Map<AccordJournal.Key, Message> writes = new HashMap<>();
    @Override
    public SerializerSupport.MessageProvider makeMessageProvider(TxnId txnId)
    {
        return new SerializerSupport.MessageProvider()
        {
            @Override
            public Set<MessageType> test(Set<MessageType> messages)
            {
                Set<AccordJournal.Key> keys = new ObjectHashSet<>(messages.size() + 1, 0.9f);
                for (MessageType message : messages)
                    for (AccordJournal.Type synonymousType : AccordJournal.Type.synonymousTypesFromMessageType(message))
                        keys.add(new AccordJournal.Key(txnId, synonymousType));
                Set<AccordJournal.Key> presentKeys = Sets.intersection(writes.keySet(), keys);
                Set<MessageType> presentMessages = new ObjectHashSet<>(presentKeys.size() + 1, 0.9f);
                for (AccordJournal.Key key : presentKeys)
                    presentMessages.add(key.type.outgoingType);
                return presentMessages;
            }

            private <T extends Message> T get(AccordJournal.Key key)
            {
                return (T) writes.get(key);
            }

            private <T extends Message> T get(MessageType messageType)
            {
                for (AccordJournal.Type type : AccordJournal.Type.synonymousTypesFromMessageType(messageType))
                {
                    T value = get(new AccordJournal.Key(txnId, type));
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
        };
    }

    @Override
    public void appendMessageBlocking(Message message)
    {
        AccordJournal.Type type = AccordJournal.Type.fromMessageType(message.type());
        AccordJournal.Key key = new AccordJournal.Key(type.txnId(message), type);
        writes.put(key, message);
    }
}
