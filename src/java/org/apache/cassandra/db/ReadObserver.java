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
package org.apache.cassandra.db;

import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;

/**
 * An interface that allows to capture what local data has been read
 * <p>
 * This is used by CNDB remote file cache warmup strategy to track access pattern
 */
public interface ReadObserver
{
    ReadObserver NO_OP = new ReadObserver() {};

    /**
     * Called on every partition read
     *
     * @param partitionKey the partition key
     * @param deletionTime partition deletion time
     */
    default void onPartition(DecoratedKey partitionKey, DeletionTime deletionTime) {}

    /**
     * Called on every static row read.
     *
     * @param staticRow static row of the partition
     */
    default void onStaticRow(Row staticRow) {}

    /**
     * Called on every unfiltered read.
     *
     * @param unfiltered either row or range tombstone.
     */
    default void onUnfiltered(Unfiltered unfiltered) {}

    /**
     * Called on read request completion
     */
    default void onComplete() {}
}
