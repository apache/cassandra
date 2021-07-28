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

package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.Future;

/**
 * The cleaner is used by {@link MemtableCleanerThread} in order to reclaim space from memtables, normally
 * by flushing the largest memtable.
 */
public interface MemtableCleaner
{
    /**
     * This is a function that schedules a cleaning task, normally flushing of the largest sstable.
     * The future will complete once the operation has completed and it will have a value set to true if
     * the cleaner was able to execute the cleaning operation or if another thread concurrently executed
     * the same clean operation. If no operation was even attempted, for example because no memtable was
     * found, then the value will be false.
     *
     * The future will complete with an error if the cleaning operation encounters an error.
     *
     */
    Future<Boolean> clean();
}