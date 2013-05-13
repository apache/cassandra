package org.apache.cassandra.utils;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


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

    /**
     * Memory a class consumes, including the object header and the size of the fields.
     * @param fieldsSize Total size of the primitive fields of a class
     * @return Total in-memory size of the class
     */
    public static long getFieldSize(long fieldsSize)
    {
        return roundTo(SPEC.getObjectHeaderSize() + fieldsSize, SPEC.getObjectPadding());
    }

    /**
     * Memory a super class consumes, given the primitive field sizes
     * @param fieldsSize Total size of the primitive fields of the super class
     * @return Total additional in-memory that the super class takes up
     */
    public static long getSuperClassFieldSize(long fieldsSize)
    {
        return roundTo(fieldsSize, SPEC.getSuperclassFieldPadding());
    }

    /**
     * Memory an array will consume
     * @param length Number of elements in the array
     * @param elementSize In-memory size of each element's primitive stored
     * @return In-memory size of the array
     */
    public static long getArraySize(int length, long elementSize)
    {
        return roundTo(SPEC.getArrayHeaderSize() + length * elementSize, SPEC.getObjectPadding());
    }

    /**
     * Memory a byte array consumes
     * @param bytes byte array to get memory size
     * @return In-memory size of the array
     */
    public static long getArraySize(byte[] bytes)
    {
        return getArraySize(bytes.length, 1);
    }

    /**
     * Memory a byte buffer consumes
     * @param buffer ByteBuffer to calculate in memory size
     * @return Total in-memory size of the byte buffer
     */
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

    public static long roundTo(long x, int multiple)
    {
        return ((x + multiple - 1) / multiple) * multiple;
    }

    /**
     * @return Memory a reference consumes on the current architecture.
     */
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
