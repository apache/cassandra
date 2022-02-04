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

package org.apache.cassandra.cache;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChunkCacheInterceptingTest
{
    Interceptor interceptor;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testChunkCacheInterception()
    {
        assumeTrue(ChunkCache.instance != null);

        try
        {
            ChunkCache.instance.intercept(rf -> intercept(rf));

            ChunkReader chunkReader = mock(ChunkReader.class);
            when(chunkReader.chunkSize()).thenReturn(1024);

            RebuffererFactory rebuferrerFactory = ChunkCache.maybeWrap(chunkReader);
            assertTrue("chunk cache didn't create our interceptor?", interceptor != null);

            rebuferrerFactory.instantiateRebufferer();
            assertEquals("interceptor not used to create rebufferer?",1, interceptor.numInstantiations);
        }
        finally
        {
            ChunkCache.instance.enable(true);
        }
    }

    private RebuffererFactory intercept(RebuffererFactory rf)
    {
        interceptor = new Interceptor(rf);
        return interceptor;
    }

    private static class Interceptor implements RebuffererFactory
    {
        private RebuffererFactory wrapped;
        private int numInstantiations = 0;

        Interceptor(RebuffererFactory wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public void close()
        {
            wrapped.close();
        }

        @Override
        public ChannelProxy channel()
        {
            return wrapped.channel();
        }

        @Override
        public long fileLength()
        {
            return wrapped.fileLength();
        }

        @Override
        public double getCrcCheckChance()
        {
            return wrapped.getCrcCheckChance();
        }

        @Override
        public Rebufferer instantiateRebufferer()
        {
            numInstantiations += 1;
            return wrapped.instantiateRebufferer();
        }

        @Override
        public void invalidateIfCached(long position)
        {
        }
    }
}