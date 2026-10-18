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
package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.utils.FBUtilities;

public class ClientWarn extends ExecutorLocals.Impl
{
    private static final String TRUNCATED = " [truncated]";
    public static ClientWarn instance = new ClientWarn();

    private ClientWarn()
    {
    }

    public State get()
    {
        return ExecutorLocals.current().clientWarnState;
    }

    public void set(State value)
    {
        ExecutorLocals current = ExecutorLocals.current();
        ExecutorLocals.Impl.set(current.traceState, value, current.eligibleForArtificialLatency);
    }

    public void warn(String text)
    {
        State state = get();
        if (state != null)
            state.add(text);
    }

    public void captureWarnings()
    {
        set(new State());
    }

    /**
     * Provides an additional control on capturing warnings. When executing SchemaTransformations in the
     * metadata log follower or when committing on a CMS member, we don't want these to be triggered.
     * @see org.apache.cassandra.schema.SchemaTransformation#enterExecution()
     **/
    public void pauseCapture()
    {
        State state = get();
        if (state != null)
            state.collecting = false;
    }

    public void resumeCapture()
    {
        State state = get();
        if (state != null)
            state.collecting = true;
    }

    public List<String> getWarnings()
    {
        State state = get();
        if (state == null || state.warnings == null || state.warnings.isEmpty())
            return null;
        return state.warnings;
    }

    public void resetWarnings()
    {
        set(null);
    }

    public static class State
    {
        private boolean collecting = true;
        // This must be a thread-safe list. Even though it's wrapped in a ThreadLocal, it's propagated to each thread
        // from shared state, so multiple threads can reference the same State.
        private volatile List<String> warnings;

        private void add(String warning)
        {
            if (warnings == null)
                synchronized (this) {
                    if (warnings == null) {
                        warnings = new CopyOnWriteArrayList<>();
                    }
                }
            if (collecting && warnings.size() < FBUtilities.MAX_UNSIGNED_SHORT)
                warnings.add(maybeTruncate(warning));
        }

        private static String maybeTruncate(String warning)
        {
            // A Java char encodes to at most 3 bytes in UTF-8 (a surrogate pair is 2 chars and 4 bytes), so a warning
            // of at most MAX_UNSIGNED_SHORT / 3 chars always fits, and we can skip computing its encoded length
            if (warning.length() <= FBUtilities.MAX_UNSIGNED_SHORT / 3
                || CBUtil.encodedUTF8Length(warning) <= FBUtilities.MAX_UNSIGNED_SHORT)
                return warning;

            // The native protocol limits a [string] to MAX_UNSIGNED_SHORT bytes, so truncate by encoded length
            // rather than by chars; the encoder stops before the first char that does not fit, so a char is never split
            int budget = FBUtilities.MAX_UNSIGNED_SHORT - TRUNCATED.length(); // TRUNCATED is ASCII
            CharBuffer in = CharBuffer.wrap(warning);
            StandardCharsets.UTF_8.newEncoder()
                                  .onMalformedInput(CodingErrorAction.REPLACE)
                                  .encode(in, ByteBuffer.allocate(budget), true);
            return warning.substring(0, in.position()) + TRUNCATED;
        }
    }
}
