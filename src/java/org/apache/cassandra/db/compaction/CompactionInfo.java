/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.compaction;

import java.io.Serializable;

/** Implements serializable to allow structured info to be returned via JMX. */
public final class CompactionInfo implements Serializable
{
    private static final long serialVersionUID = 3695381572726744816L;

    private final int id;
    private final String ksname;
    private final String cfname;
    private final OperationType tasktype;
    private final long bytesComplete;
    private final long totalBytes;

    public CompactionInfo(int id, String ksname, String cfname, OperationType tasktype, long bytesComplete, long totalBytes)
    {
        this.id = id;
        this.ksname = ksname;
        this.cfname = cfname;
        this.tasktype = tasktype;
        this.bytesComplete = bytesComplete;
        this.totalBytes = totalBytes;
    }

    /** @return A copy of this CompactionInfo with updated progress. */
    public CompactionInfo forProgress(long bytesComplete, long totalBytes)
    {
        return new CompactionInfo(id, ksname, cfname, tasktype, bytesComplete, totalBytes);
    }

    public int getId()
    {
        return id;
    }

    public String getKeyspace()
    {
        return ksname;
    }

    public String getColumnFamily()
    {
        return cfname;
    }

    public long getBytesComplete()
    {
        return bytesComplete;
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public OperationType getTaskType()
    {
        return tasktype;
    }

    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append(getTaskType()).append('@').append(id);
        buff.append('(').append(getKeyspace()).append(", ").append(getColumnFamily());
        buff.append(", ").append(getBytesComplete()).append('/').append(getTotalBytes());
        return buff.append(')').toString();
    }

    public interface Holder
    {
        public CompactionInfo getCompactionInfo();
    }
}
