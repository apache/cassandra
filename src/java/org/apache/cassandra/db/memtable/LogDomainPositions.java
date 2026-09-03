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

import java.util.Objects;

import org.apache.cassandra.db.LogDomain;
import org.apache.cassandra.db.commitlog.CommitLogPosition;

/**
 * An immutable snapshot of {@link CommitLogPosition}s for each {@link LogDomain}.
 */
public class LogDomainPositions
{
    public static final LogDomainPositions NONE = new LogDomainPositions(CommitLogPosition.NONE, CommitLogPosition.NONE);

    public final CommitLogPosition commitLog;
    public final CommitLogPosition journal;

    public LogDomainPositions(CommitLogPosition commitLog, CommitLogPosition journal)
    {
        this.commitLog = commitLog != null ? commitLog : CommitLogPosition.NONE;
        this.journal = journal != null ? journal : CommitLogPosition.NONE;
    }

    public static LogDomainPositions of(CommitLogPosition commitLog, CommitLogPosition journal)
    {
        return new LogDomainPositions(commitLog, journal);
    }

    public static LogDomainPositions of(CommitLogPosition position)
    {
        return new LogDomainPositions(position, position);
    }

    public static LogDomainPositions of(LogDomainBounds bounds)
    {
        if (bounds == null)
            return NONE;
        return new LogDomainPositions(bounds.get(LogDomain.COMMIT_LOG), bounds.get(LogDomain.MUTATION_JOURNAL));
    }

    public CommitLogPosition forDomain(LogDomain domain)
    {
        return domain.isJournal() ? journal : commitLog;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof LogDomainPositions)) return false;
        LogDomainPositions that = (LogDomainPositions) o;
        return Objects.equals(commitLog, that.commitLog) && Objects.equals(journal, that.journal);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(commitLog, journal);
    }

    @Override
    public String toString()
    {
        return "LogDomainPositions(commitLog=" + commitLog + ", journal=" + journal + ')';
    }
}
