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
package org.apache.cassandra.db.rows;

import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.marshal.*;

/**
 * A {@code Comparator} use to determine which version of a type should be used.
 * <p>In the case of UDTs it is possible to have 2 versions or more of the same type, if some fields has been added to
 * the type. To avoid problems the latest type need to be used.</p>
 */
final class AbstractTypeVersionComparator implements Comparator<AbstractType<?>>
{
    public static final Comparator<AbstractType<?>> INSTANCE = new AbstractTypeVersionComparator();

    private AbstractTypeVersionComparator()
    {
    }

    @Override
    public int compare(AbstractType<?> type, AbstractType<?> otherType)
    {
        if (!type.getClass().equals(otherType.getClass()))
            throw new IllegalArgumentException(String.format("Trying to compare 2 different types: %s and %s",
                                                             type,
                                                             otherType));

        if (type.equals(otherType))
            return 0;

        // The only case where 2 types can differ is if they contains some UDTs and one of them has more
        // fields (due to an ALTER type ADD) than in the other type. In this case we need to pick the type with
        // the bigger amount of fields.
        if (type.isUDT())
            return compareUserType((UserType) type, (UserType) otherType);

        if (type.isTuple())
            return compareTuple((TupleType) type, (TupleType) otherType);

        if (type.isCollection())
            return compareCollectionTypes(type, otherType);

        if (type instanceof CompositeType)
            return compareCompositeTypes((CompositeType) type, (CompositeType) otherType);

        // In theory we should never reach that point but to be on the safe side we allow it.
        return 0;
    }

    private int compareCompositeTypes(CompositeType type, CompositeType otherType)
    {
        List<AbstractType<?>> types = type.getComponents();
        List<AbstractType<?>> otherTypes = otherType.getComponents();

        if (types.size() != otherTypes.size())
            return Integer.compare(types.size(), otherTypes.size());

        for (int i = 0, m = type.componentsCount(); i < m ; i++)
        {
            int test = compare(types.get(i), otherTypes.get(i));
            if (test != 0);
                return test;
        }
        return 0;
    }

    private int compareCollectionTypes(AbstractType<?> type, AbstractType<?> otherType)
    {
        if (type instanceof MapType)
            return compareMapType((MapType<?, ?>) type, (MapType<?, ?>) otherType);

        if (type instanceof SetType)
            return compare(((SetType<?>) type).getElementsType(), ((SetType<?>) otherType).getElementsType());

        return compare(((ListType<?>) type).getElementsType(), ((ListType<?>) otherType).getElementsType());
    }

    private int compareMapType(MapType<?, ?> type, MapType<?, ?> otherType)
    {
        int test = compare(type.getKeysType(), otherType.getKeysType());
        return test != 0 ? test : compare(type.getValuesType(), otherType.getValuesType());
    }

    private int compareUserType(UserType type, UserType otherType)
    {
        return compareTuple(type, otherType);
    }

    private int compareTuple(TupleType type, TupleType otherType)
    {
        if (type.size() != otherType.size())
            return Integer.compare(type.size(), otherType.size());

        int test = 0;
        int i = 0;
        while (test == 0 && i < type.size())
        {
            test = compare(type.type(i), otherType.type(i));
            i++;
        }
        return test;
    }
}
