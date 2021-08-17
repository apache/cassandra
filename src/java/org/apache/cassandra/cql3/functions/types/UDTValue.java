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

/**
 * A value for a User Defined Type.
 */
public class UDTValue extends AbstractData<UDTValue>
{

    private final UserType definition;

    UDTValue(UserType definition)
    {
        super(definition.getProtocolVersion(), definition.size());
        this.definition = definition;
    }

    @Override
    protected DataType getType(int i)
    {
        return definition.byIdx[i].getType();
    }

    @Override
    protected String getName(int i)
    {
        return definition.byIdx[i].getName();
    }

    @Override
    protected CodecRegistry getCodecRegistry()
    {
        return definition.getCodecRegistry();
    }

    @Override
    protected int[] getAllIndexesOf(String name)
    {
        int[] indexes = definition.byName.get(Metadata.handleId(name));
        if (indexes == null)
            throw new IllegalArgumentException(name + " is not a field defined in this UDT");
        return indexes;
    }

    /**
     * The UDT this is a value of.
     *
     * @return the UDT this is a value of.
     */
    public UserType getType()
    {
        return definition;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDTValue)) return false;

        UDTValue that = (UDTValue) o;
        if (!definition.equals(that.definition)) return false;

        return super.equals(o);
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        TypeCodec<Object> codec = getCodecRegistry().codecFor(definition);
        sb.append(codec.format(this));
        return sb.toString();
    }
}
