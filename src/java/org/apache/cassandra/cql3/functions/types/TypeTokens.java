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

import java.util.*;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

/**
 * Utility methods to create {@code TypeToken} instances.
 */
public final class TypeTokens
{
    private TypeTokens()
    {
    }

    /**
     * Create a {@link TypeToken} that represents a {@link List} whose elements are of the given type.
     *
     * @param eltType The list element type.
     * @param <T>     The list element type.
     * @return A {@link TypeToken} that represents a {@link List} whose elements are of the given
     * type.
     */
    public static <T> TypeToken<List<T>> listOf(Class<T> eltType)
    {
        // @formatter:off
        return new TypeToken<List<T>>()
        {
        }.where(new TypeParameter<T>()
        {
        }, eltType);
        // @formatter:on
    }

    /**
     * Create a {@link TypeToken} that represents a {@link List} whose elements are of the given type.
     *
     * @param eltType The list element type.
     * @param <T>     The list element type.
     * @return A {@link TypeToken} that represents a {@link List} whose elements are of the given
     * type.
     */
    public static <T> TypeToken<List<T>> listOf(TypeToken<T> eltType)
    {
        // @formatter:off
        return new TypeToken<List<T>>()
        {
        }.where(new TypeParameter<T>()
        {
        }, eltType);
        // @formatter:on
    }

    /**
     * Create a {@link TypeToken} that represents a {@link Set} whose elements are of the given type.
     *
     * @param eltType The set element type.
     * @param <T>     The set element type.
     * @return A {@link TypeToken} that represents a {@link Set} whose elements are of the given type.
     */
    public static <T> TypeToken<Set<T>> setOf(Class<T> eltType)
    {
        // @formatter:off
        return new TypeToken<Set<T>>()
        {
        }.where(new TypeParameter<T>()
        {
        }, eltType);
        // @formatter:on
    }

    /**
     * Create a {@link TypeToken} that represents a {@link Set} whose elements are of the given type.
     *
     * @param eltType The set element type.
     * @param <T>     The set element type.
     * @return A {@link TypeToken} that represents a {@link Set} whose elements are of the given type.
     */
    public static <T> TypeToken<Set<T>> setOf(TypeToken<T> eltType)
    {
        // @formatter:off
        return new TypeToken<Set<T>>()
        {
        }.where(new TypeParameter<T>()
        {
        }, eltType);
        // @formatter:on
    }

    /**
     * Create a {@link TypeToken} that represents a {@link Map} whose keys and values are of the given
     * key and value types.
     *
     * @param keyType   The map key type.
     * @param valueType The map value type
     * @param <K>       The map key type.
     * @param <V>       The map value type
     * @return A {@link TypeToken} that represents a {@link Map} whose keys and values are of the
     * given key and value types
     */
    public static <K, V> TypeToken<Map<K, V>> mapOf(Class<K> keyType, Class<V> valueType)
    {
        // @formatter:off
        return new TypeToken<Map<K, V>>()
        {
        }.where(new TypeParameter<K>()
        {
        }, keyType)
         .where(new TypeParameter<V>()
         {
         }, valueType);
        // @formatter:on
    }

    /**
     * Create a {@link TypeToken} that represents a {@link Map} whose keys and values are of the given
     * key and value types.
     *
     * @param keyType   The map key type.
     * @param valueType The map value type
     * @param <K>       The map key type.
     * @param <V>       The map value type
     * @return A {@link TypeToken} that represents a {@link Map} whose keys and values are of the
     * given key and value types
     */
    public static <K, V> TypeToken<Map<K, V>> mapOf(TypeToken<K> keyType, TypeToken<V> valueType)
    {
        // @formatter:off
        return new TypeToken<Map<K, V>>()
        {
        }.where(new TypeParameter<K>()
        {
        }, keyType)
         .where(new TypeParameter<V>()
         {
         }, valueType);
        // @formatter:on
    }

    /**
     * Create a {@link TypeToken} that represents a {@link TypeToken} whose elements are of the given type.
     *
     * @param eltType The vector element type.
     * @param <T>     The vector element type.
     * @return A {@link TypeToken} that represents a {@link TypeToken} whose elements are of the given
     * type.
     */
    public static <T> TypeToken<List<T>> vectorOf(TypeToken<T> eltType)
    {
        // @formatter:off
        return new TypeToken<List<T>>()
        {
        }.where(new TypeParameter<T>()
        {
        }, eltType);
        // @formatter:on
    }
}
