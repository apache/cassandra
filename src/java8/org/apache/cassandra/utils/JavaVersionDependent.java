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
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaVersionDependent
{
    private static final Logger logger = LoggerFactory.getLogger(JavaVersionDependent.class);

    public static long getNioBitsTotalCapacity()
    {
        try
        {
            Class<?> bitsClass = Class.forName("java.nio.Bits");
            Field f = bitsClass.getDeclaredField("totalCapacity");
            try
            {
                f.setAccessible(true);
                AtomicLong totalCapacity = (AtomicLong) f.get(null);
                return totalCapacity.get();
            }
            finally
            {
                f.setAccessible(false);
            }
        }
        catch (NoSuchFieldException nf)
        {
            logger.debug("Could not access field java.nio.Bits.totalCapacity (Java 8)", nf);
        }
        catch (Throwable t)
        {
            logger.debug("Error accessing field of java.nio.Bits", t);
            //Don't care, will just return the dummy value -1 if we can't get at the field in this JVM
        }
        return -1L;
    }
}
