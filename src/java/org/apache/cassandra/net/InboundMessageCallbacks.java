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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.net.Message.Header;

/**
 * Encapsulates the callbacks that {@link InboundMessageHandler} invokes during the lifecycle of an inbound message
 * passing through it: from arrival to dispatch to execution.
 *
 * The flow will vary slightly between small and large messages. Small messages will be deserialized first and only
 * then dispatched to one of the {@link Stage} stages for execution, whereas a large message will be dispatched first,
 * and deserialized in-place on the relevant stage before being immediately processed.
 *
 * This difference will only show in case of deserialization failure. For large messages, it's possible for
 * {@link #onFailedDeserialize(int, Header, Throwable)} to be invoked after {@link #onExecuting(int, Header, long, TimeUnit)},
 * whereas for small messages it isn't.
 */
interface InboundMessageCallbacks
{
    /**
     * Invoked once the header of a message has arrived, small or large.
     */
    void onHeaderArrived(int messageSize, Header header, long timeElapsed, TimeUnit unit);

    /**
     * Invoked once an entire message worth of bytes has arrived, small or large.
     */
    void onArrived(int messageSize, Header header, long timeElapsed, TimeUnit unit);

    /**
     * Invoked if a message arrived too late to be processed, after its expiration. {@code wasCorrupt} might
     * be set to {@code true} if 1+ corrupt frames were encountered while assembling an expired large message.
     */
    void onArrivedExpired(int messageSize, Header header, boolean wasCorrupt, long timeElapsed, TimeUnit unit);

    /**
     * Invoked if a large message arrived in time, but had one or more of its frames corrupted in flight.
     */
    void onArrivedCorrupt(int messageSize, Header header, long timeElapsed, TimeUnit unit);

    /**
     * Invoked if {@link InboundMessageHandler} was closed before receiving all frames of a large message.
     * {@code wasCorrupt} will be set to {@code true} if some corrupt frames had been already encountered,
     * {@code wasExpired} will be set to {@code true} if the message had expired in flight.
     */
    void onClosedBeforeArrival(int messageSize, Header header, int bytesReceived, boolean wasCorrupt, boolean wasExpired);

    /**
     * Invoked if a deserializer threw an exception while attempting to deserialize a message.
     */
    void onFailedDeserialize(int messageSize, Header header, Throwable t);

    /**
     * Invoked just before a message-processing task is scheduled on the appropriate {@link Stage}
     * for the {@link Verb} of the message.
     */
    void onDispatched(int messageSize, Header header);

    /**
     * Invoked at the very beginning of execution of the message-processing task on the appropriate {@link Stage}.
     */
    void onExecuting(int messageSize, Header header, long timeElapsed, TimeUnit unit);

    /**
     * Invoked upon 'successful' processing of the message. Alternatively, {@link #onExpired(int, Header, long, TimeUnit)}
     * will be invoked if the message had expired while waiting to be processed in the queue of the {@link Stage}.
     */
    void onProcessed(int messageSize, Header header);

    /**
     * Invoked if the message had expired while waiting to be processed in the queue of the {@link Stage}. Otherwise,
     * {@link #onProcessed(int, Header)} will be invoked.
     */
    void onExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit);

    /**
     * Invoked at the very end of execution of the message-processing task, no matter the outcome of processing.
     */
    void onExecuted(int messageSize, Header header, long timeElapsed, TimeUnit unit);
}
