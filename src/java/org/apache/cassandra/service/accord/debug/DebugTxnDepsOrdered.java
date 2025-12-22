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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.primitives.Participants;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChain;
import org.apache.cassandra.service.accord.IAccordService;

import static accord.primitives.Routables.Slice.Minimal;

public class DebugTxnDepsOrdered extends DebugTxnGraph<DebugTxnGraph.TxnInfo, Set<TxnId>>
{
    public DebugTxnDepsOrdered(IAccordService service, TxnId root, TxnKindsAndDomains kinds, @Nullable Participants<?> intersecting, Timestamp min, int maxDepth, Consumer<TxnInfos<TxnInfo>> visit)
    {
        super(service, root, kinds, intersecting, min, maxDepth, visit);
    }

    public static void visit(IAccordService accord, TxnId root, TxnKindsAndDomains kinds, @Nullable Participants<?> intersecting, Timestamp min, int maxDepth, long deadlineNanos, Consumer<TxnInfos<TxnInfo>> visit) throws TimeoutException
    {
        new DebugTxnDepsOrdered(accord, root, kinds, intersecting, min, maxDepth, visit).visit(deadlineNanos);
    }

    @Override
    protected AsyncChain<TxnInfos<TxnInfo>> visitRoot(SafeCommandStore safeStore, Command command)
    {
        return visitRoot(safeStore, command, new HashSet<>());
    }

    protected TxnInfos<TxnInfo> build(CommandStore commandStore, int depth, Command parent, List<SortInfo> sortedInfos, @Nullable Participants<?> intersecting, Set<TxnId> visited)
    {
        ArrayList<TxnInfo> children = new ArrayList<>();
        visitLatestCommitted(sortedInfos, parent, (next, via) -> {
            children.add(new TxnInfo(next.txnId, next.saveStatus, next.executeAt, via));
        });
        for (int i = 0; i < sortedInfos.size() ; ++i)
        {
            SortInfo next = sortedInfos.get(i);
            if (next.saveStatus.hasBeen(Status.Committed) || !visited.add(next.txnId))
                continue;

            Participants<?> p = parent.partialDeps().participants(next.txnId);
            if (intersecting != null)
                p = p.intersecting(intersecting, Minimal);
            if (p.isEmpty()) continue;
            children.add(new TxnInfo(next.txnId, next.saveStatus, next.executeAt, p));
        }
        children.sort(Comparator.naturalOrder());
        children.trimToSize();
        return new TxnInfos<>(commandStore.id(), depth, parent.txnId(), children);
    }
}
