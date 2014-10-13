package org.apache.cassandra.utils.concurrent;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class Locks
{
    static final Unsafe unsafe;

    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    // enters the object's monitor IF UNSAFE IS PRESENT. If it isn't, this is a no-op.
    public static void monitorEnterUnsafe(Object object)
    {
        if (unsafe != null)
            unsafe.monitorEnter(object);
    }

    public static void monitorExitUnsafe(Object object)
    {
        if (unsafe != null)
            unsafe.monitorExit(object);
    }
}
