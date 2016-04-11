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

import java.net.InetSocketAddress;
import java.util.UUID;
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
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.async.TestScheduledFuture;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.CompleteMessage;

public class NettyStreamingMessageSenderTest
{
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.2", 0);

    private EmbeddedChannel channel;
    private StreamSession session;
    private NettyStreamingMessageSender sender;
    private NettyStreamingMessageSender.FileStreamTask fileStreamTask;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        channel = new EmbeddedChannel();
        channel.attr(NettyStreamingMessageSender.TRANSFERRING_FILE_ATTR).set(Boolean.FALSE);
        UUID pendingRepair = UUID.randomUUID();
        session = new StreamSession(REMOTE_ADDR.getAddress(), REMOTE_ADDR.getAddress(), (connectionId, protocolVersion) -> null, 0, true, pendingRepair, PreviewKind.ALL);
        StreamResultFuture future = StreamResultFuture.initReceivingSide(0, UUID.randomUUID(), StreamOperation.REPAIR, REMOTE_ADDR.getAddress(), channel, true, pendingRepair, session.getPreviewKind());
        session.init(future);
        sender = session.getMessageSender();
        sender.setControlMessageChannel(channel);
    }

    @After
    public void tearDown()
    {
        if (fileStreamTask != null)
            fileStreamTask.unsetChannel();
    }

    @Test
    public void KeepAliveTask_normalSend()
    {
        Assert.assertTrue(channel.isOpen());
        NettyStreamingMessageSender.KeepAliveTask task = sender.new KeepAliveTask(channel, session);
        task.run();
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void KeepAliveTask_channelClosed()
    {
        channel.close();
        Assert.assertFalse(channel.isOpen());
        channel.releaseOutbound();
        NettyStreamingMessageSender.KeepAliveTask task = sender.new KeepAliveTask(channel, session);
        task.future = new TestScheduledFuture();
        Assert.assertFalse(task.future.isCancelled());
        task.run();
        Assert.assertTrue(task.future.isCancelled());
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void KeepAliveTask_closed()
    {
        Assert.assertTrue(channel.isOpen());
        NettyStreamingMessageSender.KeepAliveTask task = sender.new KeepAliveTask(channel, session);
        task.future = new TestScheduledFuture();
        Assert.assertFalse(task.future.isCancelled());

        sender.setClosed();
        Assert.assertFalse(sender.connected());
        task.run();
        Assert.assertTrue(task.future.isCancelled());
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void KeepAliveTask_CurrentlyStreaming()
    {
        Assert.assertTrue(channel.isOpen());
        channel.attr(NettyStreamingMessageSender.TRANSFERRING_FILE_ATTR).set(Boolean.TRUE);
        NettyStreamingMessageSender.KeepAliveTask task = sender.new KeepAliveTask(channel, session);
        task.future = new TestScheduledFuture();
        Assert.assertFalse(task.future.isCancelled());

        Assert.assertTrue(sender.connected());
        task.run();
        Assert.assertFalse(task.future.isCancelled());
        Assert.assertFalse(channel.releaseOutbound());
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
        channel.attr(NettyStreamingMessageSender.TRANSFERRING_FILE_ATTR).set(Boolean.TRUE);
        fileStreamTask = sender.new FileStreamTask(null);
        fileStreamTask.injectChannel(channel);
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
        fileStreamTask.injectChannel(channel);
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
        Assert.assertNull(sender.onControlMessageComplete(promise, new CompleteMessage()));
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
        Future f = sender.onControlMessageComplete(promise, new CompleteMessage());

        f.get(5, TimeUnit.SECONDS);

        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(sender.connected());
        Assert.assertEquals(StreamSession.State.FAILED, session.state());
    }
}
