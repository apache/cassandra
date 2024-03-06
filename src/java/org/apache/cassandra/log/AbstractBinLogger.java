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

package org.apache.cassandra.log;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.binlog.BinLog;
import org.apache.cassandra.utils.concurrent.WeightedQueue;

public abstract class AbstractBinLogger<T> implements ILogger<T>
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractBinLogger.class);

    protected Map<String, String> options;
    protected volatile BinLog binLog;

    public AbstractBinLogger(Map<String, String> options)
    {
        this.options = options;
    }

    public boolean isEnabled()
    {
        return binLog != null;
    }

    /**
     * Stop the log leaving behind any generated files.
     */
    public synchronized void stop()
    {
        try
        {
            logger.info(String.format("Stopping logger %s.", this.getClass().getName()));
            if (binLog != null)
            {
                binLog.stop();
                binLog = null;
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    public static abstract class AbstractMessage extends BinLog.ReleaseableWriteMarshallable implements WeightedQueue.Weighable
    {
        protected final String message;

        public AbstractMessage(String message)
        {
            this.message = message;
        }

        protected abstract long version();

        protected abstract String type();

        public abstract void writeMarshallablePayload(WireOut wire);

        @Override
        public void release()
        {

        }

        @Override
        public int weight()
        {
            return Ints.checkedCast(ObjectSizes.sizeOf(message));
        }
    }
}
