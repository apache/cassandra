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

package org.apache.cassandra.config;

import java.util.Objects;
import java.util.function.IntSupplier;

public class PositiveInt
{
    public final int value;

    public PositiveInt(int value)
    {
        if (value < 1)
            throw new IllegalArgumentException(String.format("Only positive values are allowed; given %d", value));
        this.value = value;
    }

    private PositiveInt(int value, boolean ignored)
    {
        this.value = value;
    }

    @Override
    public String toString()
    {
        return Integer.toString(value);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PositiveInt that = (PositiveInt) o;
        return value == that.value;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    public static class UndefinedPositiveInt extends PositiveInt
    {
        public static final int UNDEFINED_VALUE = -1;
        public static final UndefinedPositiveInt UNDEFINED = new UndefinedPositiveInt(UNDEFINED_VALUE);

        public UndefinedPositiveInt(int value)
        {
            super(value, false);
            if (!(value == -1 || value >= 1))
                throw new IllegalArgumentException(String.format("Only -1 (undefined) and positive values are allowed; given %d", value));
        }

        public boolean isEnabled()
        {
            return value != UNDEFINED_VALUE;
        }

        public int or(int defaultValue)
        {
            return value == UNDEFINED_VALUE ? defaultValue : value;
        }

        public int or(IntSupplier defaultValue)
        {
            return value == UNDEFINED_VALUE ? defaultValue.getAsInt() : value;
        }
    }
}
