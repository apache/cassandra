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

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.InheritingClass;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * This class makes better sense as an inner class to SkipListMemtable (which could be as simple as
 * FACTORY = SkipListMemtable::new), but having it there causes the SkipListMemtable class to be initialized the first
 * time it is referenced (e.g. during default memtable factory construction).
 *
 * Some tests want to setup table parameters before initializing DatabaseDescriptor -- this allows them to do so, and
 * also makes sure the memtable memory pools are not created for offline tools.
 */
public class SkipListMemtableFactory implements Memtable.Factory
{
    @Override
    public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadaRef, Memtable.Owner owner)
    {
        return new SkipListMemtable(commitLogLowerBound, metadaRef, owner);
    }

    public static final SkipListMemtableFactory INSTANCE = new SkipListMemtableFactory();
    public static InheritingClass CONFIGURATION = new InheritingClass(null, SkipListMemtable.class.getName(), ImmutableMap.of());
}
