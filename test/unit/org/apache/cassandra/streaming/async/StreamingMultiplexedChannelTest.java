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

package org.apache.cassandra.streaming.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelPromise;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.TestChannel;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.messages.CompleteMessage;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.TestChannel.REMOTE_ADDR;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class StreamingMultiplexedChannelTest
{
    private NettyStreamingChannel streamingChannel;
    private TestChannel channel;
    private StreamSession session;
    private StreamingMultiplexedChannel sender;
    private StreamingMultiplexedChannel.FileStreamTask fileStreamTask;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        channel = new TestChannel();
        streamingChannel = new NettyStreamingChannel(channel, StreamingChannel.Kind.CONTROL);
        TimeUUID pendingRepair = nextTimeUUID();
        session = new StreamSession(StreamOperation.BOOTSTRAP, REMOTE_ADDR, new NettyStreamingConnectionFactory(), streamingChannel, current_version, true, 0, pendingRepair, PreviewKind.ALL);
        StreamResultFuture future = StreamResultFuture.createFollower(0, nextTimeUUID(), StreamOperation.REPAIR, REMOTE_ADDR, streamingChannel, current_version, pendingRepair, session.getPreviewKind());
        session.init(future);
        session.attachOutbound(streamingChannel);

        sender = session.getChannel();
        sender.setControlChannel(streamingChannel);
    }

    @After
    public void tearDown()
    {
        if (fileStreamTask != null)
            fileStreamTask.unsetChannel();
    }

    @Test
    public void FileStreamTask_acquirePermit_closed()
    {
        fileStreamTask = sender.new FileStreamTask(null);
        sender.setClosed();
        Assert.assertFalse(fileStreamTask.acquirePermit(1));
    }

    @Test
    public void FileStreamTask_acquirePermit_HapppyPath()
    {
        int permits = sender.semaphoreAvailablePermits();
        fileStreamTask = sender.new FileStreamTask(null);
        Assert.assertTrue(fileStreamTask.acquirePermit(1));
        Assert.assertEquals(permits - 1, sender.semaphoreAvailablePermits());
    }

    @Test
    public void FileStreamTask_BadChannelAttr()
    {
        int permits = sender.semaphoreAvailablePermits();
        channel.attr(NettyStreamingChannel.TRANSFERRING_FILE_ATTR).set(Boolean.TRUE);
        fileStreamTask = sender.new FileStreamTask(null);
        fileStreamTask.injectChannel(streamingChannel);
        fileStreamTask.run();
        Assert.assertEquals(StreamSession.State.FAILED, session.state());
        Assert.assertTrue(channel.releaseOutbound()); // when the session fails, it will send a SessionFailed msg
        Assert.assertEquals(permits, sender.semaphoreAvailablePermits());
    }

    @Test
    public void FileStreamTask_HappyPath()
    {
        int permits = sender.semaphoreAvailablePermits();
        fileStreamTask = sender.new FileStreamTask(new CompleteMessage());
        fileStreamTask.injectChannel(streamingChannel);
        fileStreamTask.run();
        Assert.assertNotEquals(StreamSession.State.FAILED, session.state());
        Assert.assertTrue(channel.releaseOutbound());
        Assert.assertEquals(permits, sender.semaphoreAvailablePermits());
    }

    @Test
    public void onControlMessageComplete_HappyPath()
    {
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(sender.connected());
        ChannelPromise promise = channel.newPromise();
        promise.setSuccess();
        Assert.assertNull(sender.onMessageComplete(promise, new CompleteMessage()));
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(sender.connected());
        Assert.assertNotEquals(StreamSession.State.FAILED, session.state());
    }

    @Test
    public void onControlMessageComplete_Exception() throws InterruptedException, ExecutionException, TimeoutException
    {
        Assert.assertTrue(channel.isOpen());
        Assert.assertTrue(sender.connected());
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new RuntimeException("this is just a testing exception"));
        Future f = sender.onMessageComplete(promise, new CompleteMessage());

        f.get(5, TimeUnit.SECONDS);

        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(sender.connected());
        Assert.assertEquals(StreamSession.State.FAILED, session.state());
    }
}
