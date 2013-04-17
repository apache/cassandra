package org.apache.cassandra.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.nio.ByteBuffer;

import org.github.jamm.MemoryMeter;

/**
 * Modified version of the code from.
 * https://github.com/twitter/commons/blob/master
 * /src/java/com/twitter/common/objectsize/ObjectSizeCalculator.java
 *
 * Difference is that we don't use reflection.
 */
public class ObjectSizes
{
    public static final MemoryLayoutSpecification SPEC = getEffectiveMemoryLayoutSpecification();
    private static final MemoryMeter meter = new MemoryMeter().omitSharedBufferOverhead();

    /**
     * Describes constant memory overheads for various constructs in a JVM
     * implementation.
     */
    public interface MemoryLayoutSpecification
    {
        int getArrayHeaderSize();

        int getObjectHeaderSize();

        int getObjectPadding();

        int getReferenceSize();

        int getSuperclassFieldPadding();
    }

    public static long getFieldSize(long fieldsSize)
    {
        return roundTo(SPEC.getObjectHeaderSize() + fieldsSize, SPEC.getObjectPadding());
    }

    public static long getSuperClassFieldSize(long fieldsSize)
    {
        return roundTo(fieldsSize, SPEC.getSuperclassFieldPadding());
    }

    public static long getArraySize(int length, long elementSize)
    {
        return roundTo(SPEC.getArrayHeaderSize() + length * elementSize, SPEC.getObjectPadding());
    }

    public static long getSizeWithRef(byte[] bytes)
    {
        return SPEC.getReferenceSize() + getArraySize(bytes.length, 1);
    }

    public static long getSize(ByteBuffer buffer)
    {
        long size = 0;
        /* BB Class */
        // final byte[] hb;
        // final int offset;
        // boolean isReadOnly;
        size += ObjectSizes.getFieldSize(1L + 4 + ObjectSizes.getReferenceSize() + ObjectSizes.getArraySize(buffer.capacity(), 1));
        /* Super Class */
        // private int mark;
        // private int position;
        // private int limit;
        // private int capacity;
        size += ObjectSizes.getSuperClassFieldSize(4L + 4 + 4 + 4 + 8);
        return size;
    }

    public static long getSizeWithRef(ByteBuffer buffer)
    {
        return SPEC.getReferenceSize() + getSize(buffer);
    }

    public static long roundTo(long x, int multiple)
    {
        return ((x + multiple - 1) / multiple) * multiple;
    }

    public static int getReferenceSize()
    {
        return SPEC.getReferenceSize();
    }

    private static MemoryLayoutSpecification getEffectiveMemoryLayoutSpecification()
    {
        final String dataModel = System.getProperty("sun.arch.data.model");
        if ("32".equals(dataModel))
        {
            // Running with 32-bit data model
            return new MemoryLayoutSpecification()
            {
                public int getArrayHeaderSize()
                {
                    return 12;
                }

                public int getObjectHeaderSize()
                {
                    return 8;
                }

                public int getObjectPadding()
                {
                    return 8;
                }

                public int getReferenceSize()
                {
                    return 4;
                }

                public int getSuperclassFieldPadding()
                {
                    return 4;
                }
            };
        }

        final String strVmVersion = System.getProperty("java.vm.version");
        final int vmVersion = Integer.parseInt(strVmVersion.substring(0, strVmVersion.indexOf('.')));
        if (vmVersion >= 17)
        {
            long maxMemory = 0;
            for (MemoryPoolMXBean mp : ManagementFactory.getMemoryPoolMXBeans())
            {
                maxMemory += mp.getUsage().getMax();
            }
            if (maxMemory < 30L * 1024 * 1024 * 1024)
            {
                // HotSpot 17.0 and above use compressed OOPs below 30GB of RAM
                // total for all memory pools (yes, including code cache).
                return new MemoryLayoutSpecification()
                {
                    public int getArrayHeaderSize()
                    {
                        return 16;
                    }

                    public int getObjectHeaderSize()
                    {
                        return 12;
                    }

                    public int getObjectPadding()
                    {
                        return 8;
                    }

                    public int getReferenceSize()
                    {
                        return 4;
                    }

                    public int getSuperclassFieldPadding()
                    {
                        return 4;
                    }
                };
            }
        }

        /* Worst case we over count. */

        // In other cases, it's a 64-bit uncompressed OOPs object model
        return new MemoryLayoutSpecification()
        {
            public int getArrayHeaderSize()
            {
                return 24;
            }

            public int getObjectHeaderSize()
            {
                return 16;
            }

            public int getObjectPadding()
            {
                return 8;
            }

            public int getReferenceSize()
            {
                return 8;
            }

            public int getSuperclassFieldPadding()
            {
                return 8;
            }
        };
    }

    public static long measureDeep(Object pojo)
    {
        return meter.measureDeep(pojo);
    }
}