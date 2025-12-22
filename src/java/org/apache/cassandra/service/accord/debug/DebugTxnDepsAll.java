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

package org.apache.cassandra.service.accord.debug;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.primitives.Participants;
import accord.primitives.Routables;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChain;
import org.apache.cassandra.service.accord.IAccordService;

import static accord.primitives.Routables.Slice.Minimal;

public class DebugTxnDepsAll extends DebugTxnGraph<DebugTxnDepsAll.TxnInfo, Map<TxnId, TxnId>>
{
    public static class TxnInfo extends DebugTxnGraph.TxnInfo
    {
        public final TxnId firstParent;

        public TxnInfo(TxnId txnId, SaveStatus saveStatus, @Nullable Timestamp executeAt, TxnId firstParent, Routables<?> via)
        {
            super(txnId, saveStatus, executeAt, via);
            this.firstParent = firstParent;
        }
    }

    public DebugTxnDepsAll(IAccordService service, TxnId root, @Nullable Participants<?> intersecting, TxnKindsAndDomains kinds, Timestamp min, int maxDepth, Consumer<TxnInfos<TxnInfo>> visit)
    {
        super(service, root, kinds, intersecting, min, maxDepth, visit);
    }

    public static void visit(IAccordService accord, TxnId root, @Nullable Participants<?> intersecting, TxnKindsAndDomains kinds, Timestamp min, int maxDepth, long deadlineNanos, Consumer<TxnInfos<TxnInfo>> visit) throws TimeoutException
    {
        new DebugTxnDepsAll(accord, root, intersecting, kinds, min, maxDepth, visit).visit(deadlineNanos);
    }

    @Override
    protected AsyncChain<TxnInfos<TxnInfo>> visitRoot(SafeCommandStore safeStore, Command command)
    {
        return visitRoot(safeStore, command, new HashMap<>());
    }

    protected TxnInfos<TxnInfo> build(CommandStore commandStore, int depth, Command parent, List<SortInfo> sortedInfos, @Nullable Participants<?> intersecting, Map<TxnId, TxnId> visited)
    {
        ImmutableList.Builder<TxnInfo> children = ImmutableList.builder();
        for (int i = 0; i < sortedInfos.size() ; ++i)
        {
            SortInfo info = sortedInfos.get(i);
            Participants<?> p = parent.partialDeps().participants(info.txnId);
            if (intersecting != null) p = p.intersecting(intersecting, Minimal);
            children.add(new TxnInfo(info.txnId, info.saveStatus, info.executeAt, visited.putIfAbsent(info.txnId, parent.txnId()), p));
        }
        return new TxnInfos<>(commandStore.id(), depth, parent.txnId(), children.build());
    }
}
