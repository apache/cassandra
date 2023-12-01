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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Abstraction over a long-indexed array of longs.
 */
public interface LongArray extends Closeable
{
    /**
     * Get value at {@code idx}.
     */
    long get(long idx);

    /**
     * Get array length.
     */
    long length();

    /**
     * @param targetToken Token to look up.  Must not be smaller than previous value queried
     *                    (the method is stateful)
     * @return The row ID of the first token equal to or greater than the target,
     *         or negative value if target token is greater than all tokens
     */
    long ceilingRowId(long targetToken);

    /**
     * Using the given value returns the first index corresponding to the value.
     *
     * @param value Value to lookup, and it must not be smaller than previous value
     * @return The index of the given value or negative value if target value is greater than all values
     */
    long indexOf(long value);

    @Override
    default void close() throws IOException { }

    class DeferredLongArray implements LongArray
    {
        private Supplier<LongArray> supplier;
        private LongArray longArray;
        private boolean opened = false;

        public DeferredLongArray(Supplier<LongArray> supplier)
        {
            this.supplier = supplier;
        }

        @Override
        public long get(long idx)
        {
            open();
            return longArray.get(idx);
        }

        @Override
        public long length()
        {
            open();
            return longArray.length();
        }

        @Override
        public long ceilingRowId(long targetToken)
        {
            open();
            return longArray.ceilingRowId(targetToken);
        }

        @Override
        public long indexOf(long targetToken)
        {
            open();
            return longArray.indexOf(targetToken);
        }

        @Override
        public void close() throws IOException
        {
            if (opened)
                longArray.close();
        }

        private void open()
        {
            if (!opened)
            {
                longArray = supplier.get();
                opened = true;
            }
        }
    }

    interface Factory
    {
        LongArray open();
    }
}
