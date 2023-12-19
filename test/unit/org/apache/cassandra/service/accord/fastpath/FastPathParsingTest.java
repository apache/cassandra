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

package org.apache.cassandra.service.accord.fastpath;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;

public class FastPathParsingTest
{
    private static void assertThrows(Runnable runnable, Class<? extends Throwable> exception)
    {
        try
        {
            runnable.run();
        }
        catch (Throwable e)
        {
            if (!exception.isAssignableFrom(e.getClass()))
            {
                throw new AssertionError(format("Expected %s to be thrown, got %s: %s", exception.getName(), e.getClass().getName(), e.getMessage()));
            }
            return;
        }
        Assert.fail(format("Expected %s to be thrown", exception.getName()));
    }

    private static Map<String, String> options(String... opts)
    {
        Assert.assertTrue("Need even numbered array for key value pairs, got " + Arrays.toString(opts), opts.length % 2 == 0);
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (int i=0; i<opts.length; i+=2)
            builder.put(opts[i], opts[i+1]);
        return builder.build();
    }

    static ParameterizedFastPathStrategy pfs(int size, String... dcs)
    {
        Map<String, String> options = new HashMap<>();
        options.put(ParameterizedFastPathStrategy.SIZE, Integer.toString(size));
        if (dcs.length > 0)
        {
            StringJoiner joiner = new StringJoiner(",");
            for (String dc : dcs)
                joiner.add(dc);
            options.put(ParameterizedFastPathStrategy.DCS, joiner.toString());
        }

        return ParameterizedFastPathStrategy.fromMap(options);
    }

    @Test
    public void fromString()
    {
        Assert.assertSame(SimpleFastPathStrategy.instance, FastPathStrategy.tableStrategyFromString("simple"));
    }

    @Test
    public void fromStringFailures()
    {
        assertThrows(() -> FastPathStrategy.tableStrategyFromString("something"), ConfigurationException.class);
    }

    @Test
    public void fromMap()
    {
        Assert.assertEquals(pfs(3), FastPathStrategy.fromMap(options("size", "3")));
        Assert.assertEquals(SimpleFastPathStrategy.instance, FastPathStrategy.fromMap(options()));
        Assert.assertEquals(pfs(1, "dc1"), FastPathStrategy.fromMap(options("size", "1", "dcs", "dc1")));
        Assert.assertEquals(pfs(3, "dc1", "dc2"), FastPathStrategy.fromMap(options("size", "3", "dcs", "dc1,dc2")));
        Assert.assertEquals(pfs(5, "dc2", "dc1"), FastPathStrategy.fromMap(options("size", "5", "dcs", "dc2,dc1")));
    }

    @Test
    public void fromMapFailures()
    {
        assertThrows(() -> FastPathStrategy.fromMap(options("dcs", "dc1")), ConfigurationException.class);
        assertThrows(() -> FastPathStrategy.fromMap(options("size", "abc")), ConfigurationException.class);
        assertThrows(() -> FastPathStrategy.fromMap(options("size", "0")), ConfigurationException.class);
        assertThrows(() -> FastPathStrategy.fromMap(options("size", "-1")), ConfigurationException.class);
        assertThrows(() -> FastPathStrategy.fromMap(options("size", "2", "dcs", " ")), ConfigurationException.class);
        assertThrows(() -> FastPathStrategy.fromMap(options("size", "5", "dcs", "dc2,dc1", "happypath", "5")), ConfigurationException.class);
    }
}
