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

package org.apache.cassandra.tcm.sequences;

import java.io.Serializable;

import com.google.common.annotations.VisibleForTesting;

/**
 *
 */
@VisibleForTesting
public abstract class SequenceState implements Serializable
{
    private static final SequenceState HALTED     = new SequenceState("Halted due to non-fatal but incorrect state"){};
    private static final SequenceState BLOCKED    = new SequenceState("Blocked on consensus from required participants"){};
    private static final SequenceState CONTINUING = new SequenceState("Continuable")
    {
        @Override
        public boolean isContinuable()
        {
            return true;
        }
    };

    public static SequenceState continuable()
    {
        return CONTINUING;
    }

    public static SequenceState halted()
    {
        return HALTED;
    }

    public static SequenceState blocked()
    {
        return BLOCKED;
    }

    public static SequenceState error(Exception cause)
    {
        return new Error(cause);
    }

    public static class Error extends SequenceState
    {
        private final RuntimeException cause;
        private Error(Throwable cause)
        {
            super("Failed due to fatal error");
            this.cause = (cause instanceof RuntimeException)
                         ? (RuntimeException) cause
                         : new RuntimeException(cause);
        }

        public RuntimeException cause()
        {
            return cause;
        }

        public boolean isError()
        {
            return true;
        }
    }

    public final String label;
    SequenceState(String label)
    {
        this.label = label;
    }

    public boolean isContinuable()
    {
        return false;
    }

    public boolean isError()
    {
        return false;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof SequenceState)) return false;

        // note: for Error instances, we don't compare the wrapped exceptions.
        // this is a bit of a hack, but SequenceState acts like an enum except
        // the Error instances are not constants as the exceptions they carry
        // are attached dynamically.
        return this.label.equals(((SequenceState) o).label);
    }

    @Override
    public int hashCode()
    {
        return label != null ? label.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return label;
    }
}
