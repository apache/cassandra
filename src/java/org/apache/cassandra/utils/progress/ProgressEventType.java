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
package org.apache.cassandra.utils.progress;

/**
 * Progress event type.
 *
 * <p>
 * Progress starts by emitting {@link #START}, followed by emitting zero or more {@link #PROGRESS} events,
 * then it emits either one of {@link #ERROR}/{@link #ABORT}/{@link #SUCCESS}.
 * Progress indicates its completion by emitting {@link #COMPLETE} at the end of process.
 * </p>
 * <p>
 * {@link #NOTIFICATION} event type is used to just notify message without progress.
 * </p>
 */
public enum ProgressEventType
{
    /**
     * Fired first when progress starts.
     * Happens only once.
     */
    START,

    /**
     * Fire when progress happens.
     * This can be zero or more time after START.
     */
    PROGRESS,

    /**
     * When observing process completes with error, this is sent once before COMPLETE.
     */
    ERROR,

    /**
     * When observing process is aborted by user, this is sent once before COMPLETE.
     */
    ABORT,

    /**
     * When observing process completes successfully, this is sent once before COMPLETE.
     */
    SUCCESS,

    /**
     * Fire when progress complete.
     * This is fired once, after ERROR/ABORT/SUCCESS is fired.
     * After this, no more ProgressEvent should be fired for the same event.
     */
    COMPLETE,

    /**
     * Used when sending message without progress.
     */
    NOTIFICATION
}
