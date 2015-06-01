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

import java.util.*;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.*;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Pair;

/**
 * Handles all repair related message.
 *
 * @since 2.0
 */
public class RepairMessageVerbHandler implements IVerbHandler<RepairMessage>
{
    private static final Logger logger = LoggerFactory.getLogger(RepairMessageVerbHandler.class);
    public void doVerb(final MessageIn<RepairMessage> message, final int id)
    {
        // TODO add cancel/interrupt message
        RepairJobDesc desc = message.payload.desc;
        try
        {
            switch (message.payload.messageType)
            {
                case PREPARE_GLOBAL_MESSAGE:
                case PREPARE_MESSAGE:
                    PrepareMessage prepareMessage = (PrepareMessage) message.payload;
                    logger.debug("Preparing, {}", prepareMessage);
                    List<ColumnFamilyStore> columnFamilyStores = new ArrayList<>(prepareMessage.cfIds.size());
                    for (UUID cfId : prepareMessage.cfIds)
                    {
                        Pair<String, String> kscf = Schema.instance.getCF(cfId);
                        ColumnFamilyStore columnFamilyStore = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);
                        columnFamilyStores.add(columnFamilyStore);
                    }
                    CassandraVersion peerVersion = SystemKeyspace.getReleaseVersion(message.from);
                    // note that we default isGlobal to true since old version always default to true:
                    boolean isGlobal = peerVersion == null ||
                                       peerVersion.compareTo(ActiveRepairService.SUPPORTS_GLOBAL_PREPARE_FLAG_VERSION) < 0 ||
                                       message.payload.messageType.equals(RepairMessage.Type.PREPARE_GLOBAL_MESSAGE);
                    logger.debug("Received prepare message: global message = {}, peerVersion = {},", message.payload.messageType.equals(RepairMessage.Type.PREPARE_GLOBAL_MESSAGE), peerVersion);
                    ActiveRepairService.instance.registerParentRepairSession(prepareMessage.parentRepairSession,
                            columnFamilyStores,
                            prepareMessage.ranges,
                            prepareMessage.isIncremental,
                            isGlobal);
                    MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
                    break;

                case SNAPSHOT:
                    logger.debug("Snapshotting {}", desc);
                    ColumnFamilyStore cfs = Keyspace.open(desc.keyspace).getColumnFamilyStore(desc.columnFamily);
                    final Range<Token> repairingRange = desc.range;
                    Set<SSTableReader> snapshottedSSSTables = cfs.snapshot(desc.sessionId.toString(), new Predicate<SSTableReader>()
                    {
                        public boolean apply(SSTableReader sstable)
                        {
                            return sstable != null &&
                                    !(sstable.partitioner instanceof LocalPartitioner) && // exclude SSTables from 2i
                                    new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(Collections.singleton(repairingRange));
                        }
                    }, true); //ephemeral snapshot, if repair fails, it will be cleaned next startup

                    Set<SSTableReader> currentlyRepairing = ActiveRepairService.instance.currentlyRepairing(cfs.metadata.cfId, desc.parentSessionId);
                    if (!Sets.intersection(currentlyRepairing, snapshottedSSSTables).isEmpty())
                    {
                        logger.error("Cannot start multiple repair sessions over the same sstables");
                        throw new RuntimeException("Cannot start multiple repair sessions over the same sstables");
                    }
                    ActiveRepairService.instance.getParentRepairSession(desc.parentSessionId).addSSTables(cfs.metadata.cfId, snapshottedSSSTables);
                    logger.debug("Enqueuing response to snapshot request {} to {}", desc.sessionId, message.from);
                    MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
                    break;

                case VALIDATION_REQUEST:
                    ValidationRequest validationRequest = (ValidationRequest) message.payload;
                    logger.debug("Validating {}", validationRequest);
                    // trigger read-only compaction
                    ColumnFamilyStore store = Keyspace.open(desc.keyspace).getColumnFamilyStore(desc.columnFamily);

                    Validator validator = new Validator(desc, message.from, validationRequest.gcBefore);
                    CompactionManager.instance.submitValidation(store, validator);
                    break;

                case SYNC_REQUEST:
                    // forwarded sync request
                    SyncRequest request = (SyncRequest) message.payload;
                    logger.debug("Syncing {}", request);
                    long repairedAt = ActiveRepairService.UNREPAIRED_SSTABLE;
                    if (desc.parentSessionId != null && ActiveRepairService.instance.getParentRepairSession(desc.parentSessionId) != null)
                        repairedAt = ActiveRepairService.instance.getParentRepairSession(desc.parentSessionId).getRepairedAt();

                    StreamingRepairTask task = new StreamingRepairTask(desc, request, repairedAt);
                    task.run();
                    break;

                case ANTICOMPACTION_REQUEST:
                    AnticompactionRequest anticompactionRequest = (AnticompactionRequest) message.payload;
                    logger.debug("Got anticompaction request {}", anticompactionRequest);
                    ListenableFuture<?> compactionDone = ActiveRepairService.instance.doAntiCompaction(anticompactionRequest.parentRepairSession, anticompactionRequest.successfulRanges);
                    compactionDone.addListener(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
                        }
                    }, MoreExecutors.sameThreadExecutor());
                    break;

                case CLEANUP:
                    logger.debug("cleaning up repair");
                    CleanupMessage cleanup = (CleanupMessage) message.payload;
                    ActiveRepairService.instance.removeParentRepairSession(cleanup.parentRepairSession);
                    MessagingService.instance().sendReply(new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE), id, message.from);
                    break;

                default:
                    ActiveRepairService.instance.handleMessage(message.from, message.payload);
                    break;
            }
        }
        catch (Exception e)
        {
            logger.error("Got error, removing parent repair session");
            if (desc != null && desc.parentSessionId != null)
                ActiveRepairService.instance.removeParentRepairSession(desc.parentSessionId);
            throw new RuntimeException(e);
        }
    }
}
