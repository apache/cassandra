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

package org.apache.cassandra.audit;

import org.junit.Test;

import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularType;
import javax.management.openmbean.OpenType;

import org.apache.commons.lang3.exception.ExceptionUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertArrayEquals;

public class AuditLogOptionsCompositeDataTest
{

    @Test
    public void testCreateMapEntryType_success() {
        CompositeType type = callCreateMapEntryType();
        assertNotNull(type);
        assertEquals("MapEntry", type.getTypeName());
        assertTrue(type.containsKey("key"));
        assertTrue(type.containsKey("value"));
    }


    @Test
    public void testCreateMapEntryType_failure_withIllegalArgumentException()
    {
        try
        {
            AuditLogOptionsCompositeData.createMapEntryType(
            new String[]{ "key", "value" },
            new String[]{ "Key" }, // fewer descriptions
            new javax.management.openmbean.OpenType[]{ SimpleType.STRING, SimpleType.STRING });
            fail("Should have thrown an exception");
        }
        catch (RuntimeException e)
        {
            assertTrue(ExceptionUtils.
                       indexOfType(e, IllegalArgumentException.class) != -1);
        }
    }

    @Test
    public void testCreateParametersType_success() {
        CompositeType entryType = callCreateMapEntryType();
        TabularType tabularType = callCreateParametersType(entryType);
        assertNotNull(tabularType);
        assertEquals("Parameters", tabularType.getTypeName());
        assertEquals(entryType, tabularType.getRowType());
        assertArrayEquals(new String[]{"key"}, tabularType.getIndexNames().toArray());
    }

    @Test
    public void testCreateParametersType_failure() {
        try
        {
            CompositeType invalidEntryType = new CompositeType("Invalid",
                                                               "Invalid entry type",
                                                               new String[]{"not_key"},
                                                               new String[]{"not a key"},
                                                               new javax.management.openmbean.OpenType[]{SimpleType.STRING});
            AuditLogOptionsCompositeData.createParametersType(invalidEntryType);
            fail("Should have thrown an exception");
        }
        catch (OpenDataException e)
        {
            fail("Should not throw OpenDataException");
        }
        catch (RuntimeException e)
        {
            assertTrue(ExceptionUtils.
                       indexOfType(e, OpenDataException.class) != -1);
        }
    }

    private static CompositeType callCreateMapEntryType() {
        return AuditLogOptionsCompositeData.createMapEntryType();
    }

    private static TabularType callCreateParametersType(CompositeType entryType) {
        return AuditLogOptionsCompositeData.createParametersType(entryType);
    }
}