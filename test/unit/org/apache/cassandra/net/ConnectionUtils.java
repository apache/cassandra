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

package org.apache.cassandra.net;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.cassandra.Util.spinAssertEquals;

public class ConnectionUtils
{
    public interface FailCheck
    {
        public void accept(String message, Long expected, Supplier<Long> actualSupplier);
    }

    public static class OutboundCountChecker
    {
        private final OutboundConnection connection;
        private long submitted;
        private long pending, pendingBytes;
        private long sent, sentBytes;
        private long overload, overloadBytes;
        private long expired, expiredBytes;
        private long error, errorBytes;
        private boolean checkSubmitted, checkPending, checkSent, checkOverload, checkExpired, checkError;

        private OutboundCountChecker(OutboundConnection connection)
        {
            this.connection = connection;
        }

        public OutboundCountChecker submitted(long count)
        {
            submitted = count;
            checkSubmitted = true;
            return this;
        }

        public OutboundCountChecker pending(long count, long bytes)
        {
            pending = count;
            pendingBytes = bytes;
            checkPending = true;
            return this;
        }

        public OutboundCountChecker sent(long count, long bytes)
        {
            sent = count;
            sentBytes = bytes;
            checkSent = true;
            return this;
        }

        public OutboundCountChecker overload(long count, long bytes)
        {
            overload = count;
            overloadBytes = bytes;
            checkOverload = true;
            return this;
        }

        public OutboundCountChecker expired(long count, long bytes)
        {
            expired = count;
            expiredBytes = bytes;
            checkExpired = true;
            return this;
        }

        public OutboundCountChecker error(long count, long bytes)
        {
            error = count;
            errorBytes = bytes;
            checkError = true;
            return this;
        }

        public void check()
        {
            doCheck((message, expected, actual) -> spinAssertEquals(message, expected, actual, 5, TimeUnit.SECONDS));
        }

        public void check(FailCheck failCheck)
        {
            doCheck((message, expect, actual) -> { if (!Objects.equals(expect, actual.get())) failCheck.accept(message, expect, actual); });
        }

        private void doCheck(FailCheck testAndFailCheck)
        {
            if (checkSubmitted)
            {
                testAndFailCheck.accept("submitted count values don't match", submitted, connection::submittedCount);
            }
            if (checkPending)
            {
                testAndFailCheck.accept("pending count values don't match", pending, () -> (long) connection.pendingCount());
                testAndFailCheck.accept("pending bytes values don't match", pendingBytes, connection::pendingBytes);
            }
            if (checkSent)
            {
                testAndFailCheck.accept("sent count values don't match", sent, connection::sentCount);
                testAndFailCheck.accept("sent bytes values don't match", sentBytes, connection::sentBytes);
            }
            if (checkOverload)
            {
                testAndFailCheck.accept("overload count values don't match", overload, connection::overloadedCount);
                testAndFailCheck.accept("overload bytes values don't match", overloadBytes, connection::overloadedBytes);
            }
            if (checkExpired)
            {
                testAndFailCheck.accept("expired count values don't match", expired, connection::expiredCount);
                testAndFailCheck.accept("expired bytes values don't match", expiredBytes, connection::expiredBytes);
            }
            if (checkError)
            {
                testAndFailCheck.accept("error count values don't match", error, connection::errorCount);
                testAndFailCheck.accept("error bytes values don't match", errorBytes, connection::errorBytes);
            }
        }
    }

    public static class InboundCountChecker
    {
        private final InboundMessageHandlers connection;
        private long scheduled, scheduledBytes;
        private long received, receivedBytes;
        private long processed, processedBytes;
        private long expired, expiredBytes;
        private long error, errorBytes;
        private boolean checkScheduled, checkReceived, checkProcessed, checkExpired, checkError;

        private InboundCountChecker(InboundMessageHandlers connection)
        {
            this.connection = connection;
        }

        public InboundCountChecker pending(long count, long bytes)
        {
            scheduled = count;
            scheduledBytes = bytes;
            checkScheduled = true;
            return this;
        }

        public InboundCountChecker received(long count, long bytes)
        {
            received = count;
            receivedBytes = bytes;
            checkReceived = true;
            return this;
        }

        public InboundCountChecker processed(long count, long bytes)
        {
            processed = count;
            processedBytes = bytes;
            checkProcessed = true;
            return this;
        }

        public InboundCountChecker expired(long count, long bytes)
        {
            expired = count;
            expiredBytes = bytes;
            checkExpired = true;
            return this;
        }

        public InboundCountChecker error(long count, long bytes)
        {
            error = count;
            errorBytes = bytes;
            checkError = true;
            return this;
        }

        public void check()
        {
            doCheck((message, expected, actual) -> spinAssertEquals(message, expected, actual, 5, TimeUnit.SECONDS));
        }

        public void check(FailCheck failCheck)
        {
            doCheck((message, expect, actual) -> { if (!Objects.equals(expect, actual.get())) failCheck.accept(message, expect, actual); });
        }

        private void doCheck(FailCheck testAndFailCheck)
        {
            if (checkReceived)
            {
                testAndFailCheck.accept("received count values don't match", received, connection::receivedCount);
                testAndFailCheck.accept("received bytes values don't match", receivedBytes, connection::receivedBytes);
            }
            if (checkProcessed)
            {
                testAndFailCheck.accept("processed count values don't match", processed, connection::processedCount);
                testAndFailCheck.accept("processed bytes values don't match", processedBytes, connection::processedBytes);
            }
            if (checkExpired)
            {
                testAndFailCheck.accept("expired count values don't match", expired, connection::expiredCount);
                testAndFailCheck.accept("expired bytes values don't match", expiredBytes, connection::expiredBytes);
            }
            if (checkError)
            {
                testAndFailCheck.accept("error count values don't match", error, connection::errorCount);
                testAndFailCheck.accept("error bytes values don't match", errorBytes, connection::errorBytes);
            }
            if (checkScheduled)
            {
                testAndFailCheck.accept("scheduled count values don't match", scheduled, connection::scheduledCount);
                testAndFailCheck.accept("scheduled bytes values don't match", scheduledBytes, connection::scheduledBytes);
            }
        }
    }

    public static OutboundCountChecker check(OutboundConnection outbound)
    {
        return new OutboundCountChecker(outbound);
    }

    public static InboundCountChecker check(InboundMessageHandlers inbound)
    {
        return new InboundCountChecker(inbound);
    }

}
