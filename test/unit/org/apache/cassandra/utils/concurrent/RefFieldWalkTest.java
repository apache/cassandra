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

package org.apache.cassandra.utils.concurrent;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.exceptions.UnaccessibleFieldException;
import org.apache.cassandra.utils.Pair;

import sun.misc.Unsafe;

import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeTrue;

/**
 * Tests the strong-reference scan when JDK access controls prevent field access. Unsafe rejects record
 * component offsets on JDK 16+. The scan must skip the field and continue.
 */
public class RefFieldWalkTest
{
    private static Unsafe unsafe()
    {
        try
        {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        }
        catch (Exception e)
        {
            return null;
        }
    }

    /** Confirms that the scan skips an unreadable JDK record component. */
    @Test
    public void nextChildSkipsUnreadableRecordField() throws Exception
    {
        Unsafe unsafe = unsafe();
        assumeTrue("Unsafe unavailable", unsafe != null);

        // Use a module-protected JDK record.
        Class<?> recordType;
        try
        {
            recordType = Class.forName("java.security.SecureClassLoader$CodeSourceKey");
        }
        catch (Throwable t)
        {
            assumeNoException("Record type not present on this JDK", t);
            return;
        }

        Object recordInstance = unsafe.allocateInstance(recordType);
        List<Field> fields = Ref.getFields(recordType);
        assumeTrue("Record has no reference component to walk", !fields.isEmpty());

        // Skip the test if JVM options make this field readable.
        boolean unreadable;
        try
        {
            Ref.getFieldValue(recordInstance, fields.get(0));
            unreadable = false;
        }
        catch (UnaccessibleFieldException expected)
        {
            unreadable = true;
        }
        assumeTrue("Field is readable in this environment; the unreadable-field skip path is not exercised", unreadable);

        Ref.InProgressVisit visit = Ref.newInProgressVisit(recordInstance, fields, null, "record");

        // This record has no other reference fields, so the scan returns no child.
        Pair<Object, Field> child = visit.nextChild();
        assertNull("Unreadable record component should be skipped, leaving no walkable children", child);
    }
}
