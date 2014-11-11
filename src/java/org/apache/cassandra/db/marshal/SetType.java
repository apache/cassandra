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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.SetSerializer;

public class SetType<T> extends CollectionType<Set<T>>
{
    // interning instances
    private static final Map<AbstractType<?>, SetType> instances = new HashMap<>();
    private static final Map<AbstractType<?>, SetType> frozenInstances = new HashMap<>();

    private final AbstractType<T> elements;
    private final SetSerializer<T> serializer;
    private final boolean isMultiCell;

    public static SetType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("SetType takes exactly 1 type parameter");

        return getInstance(l.get(0), true);
    }

    public static synchronized <T> SetType<T> getInstance(AbstractType<T> elements, boolean isMultiCell)
    {
        Map<AbstractType<?>, SetType> internMap = isMultiCell ? instances : frozenInstances;
        SetType<T> t = internMap.get(elements);
        if (t == null)
        {
            t = new SetType<T>(elements, isMultiCell);
            internMap.put(elements, t);
        }
        return t;
    }

    public SetType(AbstractType<T> elements, boolean isMultiCell)
    {
        super(Kind.SET);
        this.elements = elements;
        this.serializer = SetSerializer.getInstance(elements.getSerializer());
        this.isMultiCell = isMultiCell;
    }

    public AbstractType<T> getElementsType()
    {
        return elements;
    }

    public AbstractType<T> nameComparator()
    {
        return elements;
    }

    public AbstractType<?> valueComparator()
    {
        return EmptyType.instance;
    }

    @Override
    public boolean isMultiCell()
    {
        return isMultiCell;
    }

    @Override
    public AbstractType<?> freeze()
    {
        if (isMultiCell)
            return getInstance(this.elements, false);
        else
            return this;
    }

    @Override
    public boolean isCompatibleWithFrozen(CollectionType<?> previous)
    {
        assert !isMultiCell;
        return this.elements.isCompatibleWith(((SetType) previous).elements);
    }

    @Override
    public boolean isValueCompatibleWithFrozen(CollectionType<?> previous)
    {
        // because sets are ordered, any changes to the type must maintain the ordering
        return isCompatibleWithFrozen(previous);
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return ListType.compareListOrSet(elements, o1, o2);
    }

    public SetSerializer<T> getSerializer()
    {
        return serializer;
    }

    public boolean isByteOrderComparable()
    {
        return elements.isByteOrderComparable();
    }

    @Override
    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append("(");
        sb.append(getClass().getName());
        sb.append(TypeParser.stringifyTypeParameters(Collections.<AbstractType<?>>singletonList(elements), ignoreFreezing || !isMultiCell));
        if (includeFrozenType)
            sb.append(")");
        return sb.toString();
    }

    public List<ByteBuffer> serializedValues(List<Cell> cells)
    {
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(cells.size());
        for (Cell c : cells)
            bbs.add(c.name().collectionElement());
        return bbs;
    }
}
