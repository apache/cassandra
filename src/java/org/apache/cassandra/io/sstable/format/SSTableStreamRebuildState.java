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

package org.apache.cassandra.io.sstable.format;

import com.google.common.annotations.VisibleForTesting;

/**
 * In-memory coordination between entire-sstable (zero-copy) streaming and SAI index rebuilds for a single
 * logical sstable. Held on the shared, per-{@code Descriptor} {@code GlobalTidy}, so all reader instances of
 * the same sstable observe one authoritative status, and a crash naturally resets it on restart.
 *
 * <p>Multiple entire-sstable streams of the same sstable can run concurrently ({@code ZCS_STREAMING} with a
 * reference count), but a rebuild is exclusive ({@code REBUILDING}) and mutually exclusive with streaming. All
 * transitions are guarded by this object's own monitor, held only for the brief check-and-set; the status flag
 * itself is what persists for the duration of an operation and enforces the exclusion.
 */
public class SSTableStreamRebuildState
{
    public enum State
    {
        NORMAL,
        REBUILDING,
        ZCS_STREAMING
    }

    private State state = State.NORMAL;
    private int zcsStreamCount = 0;

    /**
     * Attempt to begin an entire-sstable stream. Fails only if a rebuild is in progress.
     *
     * @return true if streaming may proceed (caller must later call {@link #endStreaming()}), false if a rebuild
     *         is active and the caller should fall back to legacy streaming.
     */
    public synchronized boolean tryBeginStreaming()
    {
        if (state == State.REBUILDING)
            return false;
        state = State.ZCS_STREAMING;
        zcsStreamCount++;
        return true;
    }

    /**
     * Release one entire-sstable stream. Returns to {@code NORMAL} when the last stream ends. Defensive against
     * over-release so cleanup paths cannot corrupt the state.
     */
    public synchronized void endStreaming()
    {
        if (zcsStreamCount > 0 && --zcsStreamCount == 0)
            state = State.NORMAL;
    }

    /**
     * Attempt to begin a rebuild. Fails if any stream is in progress or another rebuild is already running.
     *
     * @return true if the rebuild may proceed (caller must later call {@link #endRebuild()}), false otherwise.
     */
    public synchronized boolean tryBeginRebuild()
    {
        if (state != State.NORMAL)
            return false;
        state = State.REBUILDING;
        return true;
    }

    /**
     * Release the rebuild. Defensive: only resets if currently {@code REBUILDING}.
     */
    public synchronized void endRebuild()
    {
        if (state == State.REBUILDING)
            state = State.NORMAL;
    }

    /**
     * @return the current coordination state. Useful for diagnostics/logging (for example, to report which
     *         sstables are blocking a rebuild because they are being streamed).
     */
    public synchronized State state()
    {
        return state;
    }

    @VisibleForTesting
    public synchronized int zcsStreamCount()
    {
        return zcsStreamCount;
    }
}
