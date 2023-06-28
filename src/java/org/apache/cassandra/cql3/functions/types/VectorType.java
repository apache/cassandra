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
package org.apache.cassandra.cql3.functions.types;

import java.util.Arrays;
import java.util.List;

/**
 * A vector type.
 */
public class VectorType extends DataType
{
    private final DataType subtype;
    private final int dimensions;

    VectorType(DataType subtype, int dimensions)
    {
        super(Name.VECTOR);
        assert dimensions > 0 : "vectors may only have positive dimensions; given " + dimensions;
        this.subtype = subtype;
        this.dimensions = dimensions;
    }

    public DataType getSubtype()
    {
        return this.subtype;
    }

    public int getDimensions()
    {
        return this.dimensions;
    }

    @Override
    public List<DataType> getTypeArguments()
    {
        return Arrays.asList(subtype, DataType.cint());
    }

    @Override
    public boolean isFrozen()
    {
        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VectorType that = (VectorType) o;

        if (dimensions != that.dimensions) return false;
        if (!subtype.equals(that.subtype)) return false;
        return name == that.name;
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + dimensions;
        result = 31 * result + subtype.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return String.format("vector<%s, %d>", subtype, dimensions);
    }
}
