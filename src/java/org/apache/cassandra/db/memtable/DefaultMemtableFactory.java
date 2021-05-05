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

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * This class exists solely to avoid initialization of the default memtable class.
 * Some tests want to setup table parameters before initializing DatabaseDescriptor -- this allows them to do so.
 */
public class DefaultMemtableFactory implements Memtable.Factory
{
    @Override
    public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadaRef, Memtable.Owner owner)
    {
        return TrieMemtable.FACTORY.create(commitLogLowerBound, metadaRef, owner);
    }

    @Override
    public boolean writesShouldSkipCommitLog()
    {
        return TrieMemtable.FACTORY.writesShouldSkipCommitLog();
    }

    @Override
    public boolean writesAreDurable()
    {
        return TrieMemtable.FACTORY.writesAreDurable();
    }

    @Override
    public boolean streamToMemtable()
    {
        return TrieMemtable.FACTORY.streamToMemtable();
    }

    @Override
    public boolean streamFromMemtable()
    {
        return TrieMemtable.FACTORY.streamFromMemtable();
    }

    @Override
    public TableMetrics.ReleasableMetric createMemtableMetrics(TableMetadataRef metadataRef)
    {
        return TrieMemtable.FACTORY.createMemtableMetrics(metadataRef);
    }
}
