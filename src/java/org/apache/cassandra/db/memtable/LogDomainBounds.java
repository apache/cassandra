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

package org.apache.cassandra.db.memtable;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.LogDomain;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.replication.MutationJournal;

/**
 * One commit log position per {@link LogDomain}, at the boundary between two memtable generations.
 *
 * The same instance is the upper bound of the generation being switched out and the lower bound of its replacement, so
 * the two generations' spans in each log meet without a gap and without overlapping.
 */
public class LogDomainBounds
{
    private static final Function<LogDomain, CommitLogPosition> LIVE_LOGS =
        domain -> domain.isJournal() ? MutationJournal.currentPositionOrNull()
                                     : CommitLog.instance.getCurrentPosition();

    private final AtomicReference<CommitLogPosition> commitLog;
    private final AtomicReference<CommitLogPosition> journal;

    private LogDomainBounds(CommitLogPosition commitLog, CommitLogPosition journal)
    {
        this.commitLog = new AtomicReference<>(commitLog);
        this.journal = new AtomicReference<>(journal);
    }

    public static LogDomainBounds unset()
    {
        return new LogDomainBounds(null, null);
    }

    public static LogDomainBounds atCurrentPositions()
    {
        return new LogDomainBounds(CommitLog.instance.getCurrentPosition(), MutationJournal.currentPositionOrNull());
    }

    public static LogDomainBounds of(CommitLogPosition position)
    {
        return new LogDomainBounds(position, position);
    }

    public AtomicReference<CommitLogPosition> forDomain(LogDomain domain)
    {
        return domain.isJournal() ? journal : commitLog;
    }

    public CommitLogPosition get(LogDomain domain)
    {
        return forDomain(domain).get();
    }

    public boolean isSealed(LogDomain domain)
    {
        return get(domain) instanceof Memtable.LastCommitLogPosition;
    }

    /**
     * Fix each bound at the end of the log it names, so that no write can take a position at or below it afterwards.
     */
    public void seal()
    {
        seal(LIVE_LOGS);
    }

    public AtomicReference<CommitLogPosition> sealIfUnset(LogDomain domain)
    {
        return sealIfUnset(domain, LIVE_LOGS);
    }

    @VisibleForTesting
    void seal(Function<LogDomain, CommitLogPosition> logs)
    {
        seal(commitLog, LogDomain.COMMIT_LOG, logs);
        seal(journal, LogDomain.MUTATION_JOURNAL, logs);
    }

    @VisibleForTesting
    AtomicReference<CommitLogPosition> sealIfUnset(LogDomain domain, Function<LogDomain, CommitLogPosition> logs)
    {
        AtomicReference<CommitLogPosition> bound = forDomain(domain);
        if (bound.get() == null)
            seal(bound, domain, logs);
        return bound;
    }

    private static void seal(AtomicReference<CommitLogPosition> bound,
                             LogDomain domain,
                             Function<LogDomain, CommitLogPosition> logs)
    {
        while (true)
        {
            // Re-read on every attempt. A write admitted since the last read raises the bound above the position we
            // hold, and retrying with that stale position could never satisfy the guard below.
            CommitLogPosition position = logs.apply(domain);
            if (position == null)
                return;

            Memtable.LastCommitLogPosition sealed = new Memtable.LastCommitLogPosition(position);
            CommitLogPosition current = bound.get();
            if ((current == null || current.compareTo(sealed) <= 0) && bound.compareAndSet(current, sealed))
                return;
        }
    }

    @Override
    public String toString()
    {
        return "DomainBounds(commitLog=" + commitLog.get() + ", journal=" + journal.get() + ')';
    }
}
