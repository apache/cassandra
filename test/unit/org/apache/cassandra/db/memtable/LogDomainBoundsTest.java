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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;

import org.apache.cassandra.db.LogDomain;
import org.apache.cassandra.db.commitlog.CommitLogPosition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LogDomainBoundsTest
{
    @Test(timeout = 30_000)
    public void basicSeal()
    {
        LogDomainBounds bounds = LogDomainBounds.unset();

        bounds.seal(positions(new CommitLogPosition(1, 100), new CommitLogPosition(9000, 0)));

        assertEquals(sealedAt(1, 100), bounds.get(LogDomain.COMMIT_LOG));
        assertEquals(sealedAt(9000, 0), bounds.get(LogDomain.MUTATION_JOURNAL));
    }

    @Test(timeout = 30_000)
    public void sealWithRacingWrite()
    {
        LogDomainBounds bounds = LogDomainBounds.unset();
        AtomicInteger reads = new AtomicInteger();

        // The first read plants a higher position behind it, standing in for the racing write.
        Function<LogDomain, CommitLogPosition> log = domain -> {
            int read = reads.incrementAndGet();
            if (read == 1)
                bounds.forDomain(LogDomain.COMMIT_LOG).set(new CommitLogPosition(1, 500));
            return new CommitLogPosition(1, read == 1 ? 100 : 600);
        };

        bounds.sealIfUnset(LogDomain.COMMIT_LOG, log);

        // Two reads means the loop re-read exactly once
        assertEquals(2, reads.get());
        assertEquals(sealedAt(1, 600), bounds.get(LogDomain.COMMIT_LOG));
    }

    /**
     * seal shouldn't set positions for log domains not in use
     */
    @Test(timeout = 30_000)
    public void singleDomainSeal()
    {
        LogDomainBounds bounds = LogDomainBounds.unset();

        // The state before the journal starts: it reports no position, while the commit log still has one.
        bounds.seal(positions(new CommitLogPosition(1, 100), null));

        assertEquals(sealedAt(1, 100), bounds.get(LogDomain.COMMIT_LOG));
        assertNull(bounds.get(LogDomain.MUTATION_JOURNAL));
    }

    /** sealIfUnset must not move a bound the previous flush already fixed, or two adjacent spans overlap. */
    @Test(timeout = 30_000)
    public void sealIfUnset()
    {
        LogDomainBounds bounds = LogDomainBounds.unset();
        CommitLogPosition inherited = new CommitLogPosition(7, 42);
        bounds.forDomain(LogDomain.COMMIT_LOG).set(inherited);

        bounds.sealIfUnset(LogDomain.COMMIT_LOG, positions(new CommitLogPosition(7, 99), null));

        // commit log domain was already set, so nothing should have changed
        assertEquals(inherited, bounds.get(LogDomain.COMMIT_LOG));
    }

    private static Function<LogDomain, CommitLogPosition> positions(CommitLogPosition commitLog, CommitLogPosition journal)
    {
        return domain -> domain.isJournal() ? journal : commitLog;
    }

    private static Memtable.LastCommitLogPosition sealedAt(long segmentId, int position)
    {
        return new Memtable.LastCommitLogPosition(new CommitLogPosition(segmentId, position));
    }
}
