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

package org.apache.cassandra.nodes;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;

import org.apache.cassandra.db.commitlog.CommitLogPosition;

public final class TruncationRecord
{
    public final CommitLogPosition position;
    public final long truncatedAt;

    public TruncationRecord(CommitLogPosition position, long truncatedAt)
    {
        this.position = position;
        this.truncatedAt = truncatedAt;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TruncationRecord that = (TruncationRecord) o;
        return truncatedAt == that.truncatedAt &&
               Objects.equals(position, that.position);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(position, truncatedAt);
    }


    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
        .append("position", position)
        .append("truncatedAt", truncatedAt)
        .toString();
    }
}
