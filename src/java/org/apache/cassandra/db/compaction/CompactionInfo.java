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
package org.apache.cassandra.db.compaction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;

/** Implements serializable to allow structured info to be returned via JMX. */
public final class CompactionInfo implements Serializable
{
    private static final long serialVersionUID = 3695381572726744816L;
    private final CFMetaData cfm;
    private final OperationType tasktype;
    private final long completed;
    private final long total;
    private final String unit;

    public CompactionInfo(CFMetaData cfm, OperationType tasktype, long bytesComplete, long totalBytes)
    {
        this(cfm, tasktype, bytesComplete, totalBytes, "bytes");
    }

    public CompactionInfo(OperationType tasktype, long completed, long total, String unit)
    {
        this(null, tasktype, completed, total, unit);
    }

    public CompactionInfo(CFMetaData cfm, OperationType tasktype, long completed, long total, String unit)
    {
        this.tasktype = tasktype;
        this.completed = completed;
        this.total = total;
        this.cfm = cfm;
        this.unit = unit;
    }

    /** @return A copy of this CompactionInfo with updated progress. */
    public CompactionInfo forProgress(long complete, long total)
    {
        return new CompactionInfo(cfm, tasktype, complete, total, unit);
    }

    public UUID getId()
    {
        return cfm.cfId;
    }

    public String getKeyspace()
    {
        return cfm.ksName;
    }

    public String getColumnFamily()
    {
        return cfm.cfName;
    }

    public CFMetaData getCFMetaData()
    {
        return cfm;
    }

    public long getCompleted()
    {
        return completed;
    }

    public long getTotal()
    {
        return total;
    }

    public OperationType getTaskType()
    {
        return tasktype;
    }

    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append(getTaskType()).append('@').append(getId());
        buff.append('(').append(getKeyspace()).append(", ").append(getColumnFamily());
        buff.append(", ").append(getCompleted()).append('/').append(getTotal());
        return buff.append(')').append(unit).toString();
    }

    public Map<String, String> asMap()
    {
        Map<String, String> ret = new HashMap<String, String>();
        ret.put("id", getId() == null ? "" : getId().toString());
        ret.put("keyspace", getKeyspace());
        ret.put("columnfamily", getColumnFamily());
        ret.put("completed", Long.toString(completed));
        ret.put("total", Long.toString(total));
        ret.put("taskType", tasktype.toString());
        ret.put("unit", unit);
        return ret;
    }

    public static abstract class Holder
    {
        private volatile boolean stopRequested = false;
        public abstract CompactionInfo getCompactionInfo();
        double load = StorageMetrics.load.getCount();
        double reportedSeverity = 0d;

        public void stop()
        {
            stopRequested = true;
        }

        public boolean isStopRequested()
        {
            return stopRequested;
        }
        /**
         * report event on the size of the compaction.
         */
        public void started()
        {
            reportedSeverity = getCompactionInfo().getTotal() / load;
            StorageService.instance.reportSeverity(reportedSeverity);
        }

        /**
         * remove the event complete
         */
        public void finished()
        {
            if (reportedSeverity != 0d)
                StorageService.instance.reportSeverity(-(reportedSeverity));
            reportedSeverity = 0d;
        }
    }
}
