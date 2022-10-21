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

package org.apache.cassandra.service.accord.store;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.service.accord.AccordState;

public class StoredValueTest
{
    static void assertISE(Runnable runnable)
    {
        try
        {
            runnable.run();
            Assert.fail("Expected IllegalStateException");
        }
        catch (IllegalStateException e)
        {
            // noop
        }
    }

    @Test
    public void storedValueTest()
    {
        StoredValue<Integer> value = new StoredValue<>(AccordState.ReadWrite.FULL);
        // value is unloaded, read should fail
        assertISE(value::get);

        value.load(5);
        Assert.assertFalse(value.hasModifications());
        Assert.assertEquals(Integer.valueOf(5), value.get());

        value.set(6);
        Assert.assertTrue(value.hasModifications());
        Assert.assertEquals(Integer.valueOf(6), value.get());

        // loading into an unsaved field should fail
        assertISE(() -> value.load(7));

        value.clearModifiedFlag();
        Assert.assertFalse(value.hasModifications());
        Assert.assertEquals(Integer.valueOf(6), value.get());

        value.unload();
        // value is unloaded again, read should fail
        assertISE(() -> value.get());
    }

    @Test
    public void historyPreservingTest()
    {
        StoredValue.HistoryPreserving<Integer> value = new StoredValue.HistoryPreserving<>(AccordState.ReadWrite.FULL);
        value.load(5);

        Assert.assertEquals(Integer.valueOf(5), value.get());
        Assert.assertEquals(Integer.valueOf(5), value.previous());

        value.set(6);
        Assert.assertEquals(Integer.valueOf(6), value.get());
        Assert.assertEquals(Integer.valueOf(5), value.previous());

        value.clearModifiedFlag();
        Assert.assertEquals(Integer.valueOf(6), value.get());
        Assert.assertEquals(Integer.valueOf(6), value.previous());
    }
}
