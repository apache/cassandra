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

import org.apache.cassandra.utils.NonThrowingCloseable;

/**
 * An observer of {@link AbstractTableOperation}.
 * <p/>
 * The observer is notified when an operation is started. It returns a closeable that will be closed
 * when the operation is finished. The operation can be queried at any time to get the progress information.
 */
public interface TableOperationObserver
{
    TableOperationObserver NOOP = operation -> () -> {};

    /**
     * Signal to the observer that an operation is starting.
     *
     * @param operation the operation starting
     *
     * @return a closeable that the caller should close when the operation completes
     */
    NonThrowingCloseable onOperationStart(TableOperation operation);
}