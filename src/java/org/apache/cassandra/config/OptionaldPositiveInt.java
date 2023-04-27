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

public class OptionaldPositiveInt
{
    public static final int UNDEFINED_VALUE = -1;
    public static final OptionaldPositiveInt UNDEFINED = new OptionaldPositiveInt(UNDEFINED_VALUE);

    private final int value;

    public OptionaldPositiveInt(int value)
    {
        if (!(value == -1 || value >= 1))
            throw new IllegalArgumentException(String.format("Only -1 (undefined) and positive values are allowed; given %d", value));
        this.value = value;
    }

    public boolean isDefined()
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OptionaldPositiveInt that = (OptionaldPositiveInt) o;
        return value == that.value;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    @Override
    public String toString()
    {
        return !isDefined() ? "null" : Integer.toString(value);
    }
}
