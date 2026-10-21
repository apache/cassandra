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

package org.apache.cassandra.io.sstable;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.validateClusteringValueLength;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins the length check the cursor applies to a clustering value and to a cell path.
 *
 * The check mirrors AbstractType.read. Both arms matter: readUnsignedVInt32 can return a negative
 * int, which reaches DataInput.skipBytes as a silent no-op and a buffer sizer as a defect, and an
 * absurd positive length would size an array from damaged bytes.
 */
public class CursorClusteringLengthValidationTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void acceptsALengthTheWireCouldHaveProduced() throws IOException
    {
        validateClusteringValueLength(0);
        validateClusteringValueLength(1);
        validateClusteringValueLength(DatabaseDescriptor.getMaxValueSize());
    }

    @Test
    public void rejectsANegativeLength()
    {
        assertRejected(-1, "negative");
        assertRejected(Integer.MIN_VALUE, "negative");
    }

    @Test
    public void rejectsALengthOverTheConfiguredMaximum()
    {
        assertRejected(DatabaseDescriptor.getMaxValueSize() + 1, "max_value_size");
        assertRejected(Integer.MAX_VALUE, "max_value_size");
    }

    private static void assertRejected(int length, String expectedInMessage)
    {
        try
        {
            validateClusteringValueLength(length);
            fail("expected length " + length + " to be rejected");
        }
        catch (IOException e)
        {
            assertTrue("message should mention " + expectedInMessage + ", was: " + e.getMessage(),
                       e.getMessage().contains(expectedInMessage));
        }
    }
}
