/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.concurrent;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.utils.ObjectSizes;

public class RefCountedTest
{

    private static final class Tidier implements RefCounted.Tidy
    {
        boolean tidied;

        public void tidy()
        {
            tidied = true;
        }

        public String name()
        {
            return "test tidy";
        }
    }

    @Test
    public void testLeak() throws InterruptedException
    {
        Tidier tidier = new Tidier();
        Ref<?> obj = new Ref(null, tidier);
        obj.tryRef();
        obj.release();
        System.gc();
        System.gc();
        Thread.sleep(1000);
        Assert.assertTrue(tidier.tidied);
    }

    @Test
    public void testSeriousLeak() throws InterruptedException
    {
        Tidier tidier = new Tidier();
        new Ref(null, tidier);
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        Thread.sleep(1000);
        Assert.assertTrue(tidier.tidied);
    }

    @Test
    public void testDoubleRelease() throws InterruptedException
    {
        Tidier tidier = null;
        try
        {
            tidier = new Tidier();
            Ref<?> obj = new Ref(null, tidier);
            obj.release();
            obj.release();
            Assert.assertTrue(false);
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testMemoryLeak()
    {
        Tidier tidier = new Tidier();
        Ref<Object> ref = new Ref(null, tidier);
        long initialSize = ObjectSizes.measureDeep(ref);
        for (int i = 0 ; i < 1000 ; i++)
            ref.ref().release();
        long finalSize = ObjectSizes.measureDeep(ref);
        if (finalSize > initialSize * 2)
            throw new AssertionError();
        ref.release();
    }
}
