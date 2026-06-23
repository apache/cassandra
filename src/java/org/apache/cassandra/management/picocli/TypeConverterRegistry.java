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

package org.apache.cassandra.management.picocli;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class TypeConverterRegistry
{
    private static final Map<Class<?>, TypeConverter<?>> converters = new LinkedHashMap<>();

    static
    {
        register(boolean.class, (TypeConverter<Boolean>) Boolean::parseBoolean);
        register(int.class, (TypeConverter<Integer>) Integer::parseInt);
        register(long.class, (TypeConverter<Long>) Long::parseLong);
        register(double.class, (TypeConverter<Double>) Double::parseDouble);
        register(float.class, (TypeConverter<Float>) Float::parseFloat);
        register(byte.class, (TypeConverter<Byte>) Byte::parseByte);
        register(short.class, (TypeConverter<Short>) Short::parseShort);
        register(char.class, value -> {
            if (value.length() != 1)
                throw new IllegalArgumentException("Cannot convert to char: " + value);
            return value.charAt(0);
        });
        register(Boolean.class, (TypeConverter<Boolean>) Boolean::parseBoolean);
        register(Integer.class, (TypeConverter<Integer>) Integer::parseInt);
        register(Long.class, (TypeConverter<Long>) Long::parseLong);
        register(Double.class, (TypeConverter<Double>) Double::parseDouble);
        register(Float.class, (TypeConverter<Float>) Float::parseFloat);
        register(Byte.class, (TypeConverter<Byte>) Byte::parseByte);
        register(Short.class, (TypeConverter<Short>) Short::parseShort);
        register(String.class, (TypeConverter<String>) value -> value);
        register(Character.class, value -> {
            if (value.length() != 1)
                throw new IllegalArgumentException("Cannot convert to Character: " + value);
            return value.charAt(0);
        });
        register(Object.class, value -> value);
    }

    private static void register(Class<?> type, TypeConverter<?> converter)
    {
        converters.put(type, converter);
    }

    public static <T> Object convertValueBasic(Object value, Class<T> targetType) throws Exception
    {
        return convertValueBasic(value, targetType, null);
    }

    public static <T> Object convertValueBasic(Object value, Class<T> targetType, Class<?> elementType) throws Exception
    {
        if (value == null)
            return null;

        // Arrays and collections must be handled before the assignable short-circuit below: a
        // List<String> is already assignable to List, but its elements still need per-element
        // conversion when the declared element type is not String.
        if (targetType.isArray() || Collection.class.isAssignableFrom(targetType))
            return convertToArrayOrCollection(value, targetType, elementType);

        if (targetType.isAssignableFrom(value.getClass()))
            return value;

        String stringValue = value.toString();
        if (isBasicType(targetType))
        {
            TypeConverter<?> converter = converters.get(targetType);
            if (converter == null)
                throw new IllegalStateException(String.format("No converter registered for basic type: %s",
                                                              targetType.getName()));
            return converter.convert(stringValue);
        }

        if (targetType.isEnum())
            return getEnumTypeConverter(targetType).convert(stringValue);

        throw new IllegalArgumentException(String.format("No converter available for type: %s.", targetType.getName()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> TypeConverter<T> getEnumTypeConverter(Class<T> type)
    {
        return value -> {
            for (T constant : type.getEnumConstants())
                if (((Enum<?>) constant).name().equalsIgnoreCase(value))
                    return constant;
            throw new IllegalArgumentException("No enum constant " + type.getCanonicalName() + '.' + value);
        };
    }

    private static Object convertToArrayOrCollection(Object value, Class<?> targetType, Class<?> elementType) throws Exception
    {
        Collection<?> sourceCollection;
        if (value instanceof Collection)
            sourceCollection = (Collection<?>) value;
        else if (value.getClass().isArray())
        {
            int length = Array.getLength(value);
            List<Object> list = new ArrayList<>(length);
            for (int i = 0; i < length; i++)
                list.add(Array.get(value, i));
            sourceCollection = list;
        }
        else
            throw new IllegalArgumentException("Cannot convert " + value.getClass() + " to " + targetType);

        if (targetType.isArray())
        {
            Class<?> componentType = targetType.getComponentType();
            Object targetArray = Array.newInstance(componentType, sourceCollection.size());
            int index = 0;
            for (Object item : sourceCollection)
            {
                Object convertedItem = convertValueBasic(item, componentType);
                Array.set(targetArray, index++, convertedItem);
            }
            return targetArray;
        }

        Collection<Object> targetCollection;
        if (targetType == List.class || List.class.isAssignableFrom(targetType))
            targetCollection = new ArrayList<>();
        else if (SortedSet.class.isAssignableFrom(targetType))
            targetCollection = new TreeSet<>();
        else if (targetType == Set.class || Set.class.isAssignableFrom(targetType))
            targetCollection = new LinkedHashSet<>();
        else
            targetCollection = new ArrayList<>();

        if (elementType == null)
            targetCollection.addAll(sourceCollection);
        else
            for (Object item : sourceCollection)
                targetCollection.add(convertValueBasic(item, elementType));

        return targetCollection;
    }

    private static boolean isBasicType(Class<?> type)
    {
        return type == String.class ||
               type == boolean.class || type == Boolean.class ||
               type == int.class || type == Integer.class ||
               type == long.class || type == Long.class ||
               type == double.class || type == Double.class ||
               type == float.class || type == Float.class ||
               type == byte.class || type == Byte.class ||
               type == short.class || type == Short.class ||
               type == char.class || type == Character.class;
    }
}
