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

package org.apache.cassandra.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

import sun.misc.Unsafe;

public class ReflectionUtils
{
    private ReflectionUtils()
    {

    }

    private static final Unsafe UNSAFE = theUnsafe();

    private static Unsafe theUnsafe()
    {
        try
        {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        }
        catch (ReflectiveOperationException e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Writes a field through {@link Unsafe}, including instance and static {@code final} fields.
     * Clearing the {@code Field.modifiers} bit does not permit these writes on JDK 22+.
     * <p>
     * This cannot change a {@code static final} constant inlined by the compiler.
     *
     * @param instance the instance whose field to set, or {@code null} for a static field
     * @param field    the field to write, which may be {@code final}
     * @param value    the new value, boxed for primitive fields
     */
    public static void writeField(Object instance, Field field, Object value)
    {
        boolean isStatic = Modifier.isStatic(field.getModifiers());
        Object base = isStatic ? UNSAFE.staticFieldBase(field) : instance;
        long offset = isStatic ? UNSAFE.staticFieldOffset(field) : UNSAFE.objectFieldOffset(field);
        Class<?> type = field.getType();
        if (!type.isPrimitive())
            UNSAFE.putObject(base, offset, value);
        else if (type == boolean.class)
            UNSAFE.putBoolean(base, offset, (Boolean) value);
        else if (type == byte.class)
            UNSAFE.putByte(base, offset, (Byte) value);
        else if (type == char.class)
            UNSAFE.putChar(base, offset, (Character) value);
        else if (type == short.class)
            UNSAFE.putShort(base, offset, (Short) value);
        else if (type == int.class)
            UNSAFE.putInt(base, offset, (Integer) value);
        else if (type == long.class)
            UNSAFE.putLong(base, offset, (Long) value);
        else if (type == float.class)
            UNSAFE.putFloat(base, offset, (Float) value);
        else if (type == double.class)
            UNSAFE.putDouble(base, offset, (Double) value);
        else
            throw new IllegalArgumentException("Unsupported field type: " + type);
    }

    public static Field getModifiersField() throws NoSuchFieldException
    {
        return getField(Field.class, "modifiers");
    }

    public static Field getField(Class<?> clazz, String fieldName) throws NoSuchFieldException
    {
        // below code works before Java 12
        try
        {
            return clazz.getDeclaredField(fieldName);
        }
        catch (NoSuchFieldException e)
        {
            // this is mitigation for JDK 17 (https://bugs.openjdk.org/browse/JDK-8210522)
            try
            {
                Method getDeclaredFields0 = Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class);
                getDeclaredFields0.setAccessible(true);
                Field[] fields = (Field[]) getDeclaredFields0.invoke(clazz, false);
                for (Field field : fields)
                {
                    if (fieldName.equals(field.getName()))
                    {
                        return field;
                    }
                }
            }
            catch (ReflectiveOperationException ex)
            {
                e.addSuppressed(ex);
            }
            throw e;
        }
    }

    /**
     * Used by the in-jvm dtest framework to remove entries from private map fields that otherwise would prevent
     * collection of classloaders (which causes metaspace OOMs) or otherwise interfere with instance restart.
     * @param clazz The class which has the map field to clear
     * @param instance an instance of the class to clear (pass null for a static member)
     * @param mapName the name of the map field to clear
     * @param shouldRemove a predicate which determines if the entry in question should be removed
     * @param <K> The type of the map key
     * @param <V> The type of the map value
     */
    public static <K, V> void clearMapField(Class<?> clazz, Object instance, String mapName, Predicate<Map.Entry<K, V>> shouldRemove) {
        try
        {
            Field mapField = getField(clazz, mapName);
            mapField.setAccessible(true);
            // noinspection unchecked
            Map<K, V> map = (Map<K, V>) mapField.get(instance);
            // Because multiple instances can be shutting down at once,
            // synchronize on the map to avoid ConcurrentModificationException
            synchronized (map)
            {
                // This could be done with a simple `map.entrySet.removeIf()` call
                // but for debugging purposes it is much easier to keep it like this.
                Iterator<Map.Entry<K,V>> it = map.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<K,V> entry = it.next();
                    if (shouldRemove.test(entry))
                    {
                        it.remove();
                    }
                }
            }
        }
        catch (NoSuchFieldException | IllegalAccessException ex)
        {
            throw new RuntimeException(String.format("Could not clear map field %s in class %s", mapName, clazz), ex);
        }
    }

    public static <T> void setFieldToNull(T object, Class<T> declaringClass, String fieldName)
    {
        try
        {
            Field field = declaringClass.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, null);
        }
        catch (NoSuchFieldException e)
        {
            throw new RuntimeException(String.format("Could not find field %s in %s", fieldName, object.getClass()));
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(String.format("Could not set field %s in %s", fieldName, object.getClass()), e);
        }
    }
}
