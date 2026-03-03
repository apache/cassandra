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

package org.apache.cassandra.db.lifecycle;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static java.util.Collections.emptySet;
import static org.apache.cassandra.db.lifecycle.View.permitCompacting;
import static org.apache.cassandra.db.lifecycle.View.updateCompacting;

/**
 * A {@link LifecycleTransaction} subclass that exposes the package-private constructor
 * for use in tests. This allows tests to create anonymous subclasses that override
 * {@link #checkpoint()} and {@link #doCommit(Throwable)} for verification purposes.
 * <p>
 * The constructor marks the given SSTables as compacting in the tracker (equivalent to
 * what {@link Tracker#tryModify(Iterable, OperationType)} does) before delegating to
 * the parent constructor.
 */
public class TestableLifecycleTransaction extends LifecycleTransaction
{
    public TestableLifecycleTransaction(Tracker tracker, OperationType operationType, Iterable<SSTableReader> readers)
    {
        super(markCompactingAndReturn(tracker, readers), operationType, readers);
    }

    private static Tracker markCompactingAndReturn(Tracker tracker, Iterable<SSTableReader> readers)
    {
        tracker.apply(permitCompacting(readers), updateCompacting(emptySet(), readers));
        return tracker;
    }
}
