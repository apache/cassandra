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
package org.apache.cassandra.journal;

public interface AsyncCallbacks<K, V>
{
    /**
     * Invoked once an entry has been written to the file, and indexes have been updated, but before it
     * has been flushed to disk. Invoked from the writer thread. Execution order of onWrite() callbacks
     * with regard to each other is undefined.
     */
    void onWrite(long segment, int position, int size, K key, V value, Object writeContext);

    /**
     * Invoked when anything goes wrong with writing the entry - anywhere from serialization to writing to the file,
     * to requesting the flush.
     */
    void onWriteFailed(K key, V value, Object writeContext, Throwable cause);

    /**
     * Invoked after {@link Flusher} successfully flushes a segment or multiple segments to disk.
     * Invocation of this callback implies that any segments older than {@code segment} have been
     * completed and also flushed.
     * Invocation of this callback also implies that all {@link #onWrite(long, int, int, Object, Object, Object)}
     * callbacks for all entries earlier than (segment, position) have finished execution.
     */
    void onFlush(long segment, int position);

    void onFlushFailed(Throwable cause);
}
