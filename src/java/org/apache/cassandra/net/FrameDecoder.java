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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;

import com.google.common.annotations.VisibleForTesting;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;

import static org.apache.cassandra.utils.ByteBufferUtil.copyBytes;

/**
 * A Netty inbound handler that decodes incoming frames and passes them forward to
 * {@link InboundMessageHandler} for processing.
 *
 * Handles work stashing, and together with {@link InboundMessageHandler} - flow control.
 *
 * Unlike most Netty inbound handlers, doesn't use the pipeline to talk to its
 * upstream handler. Instead, a {@link FrameProcessor} must be registered with
 * the frame decoder, to be invoked on new frames. See {@link #deliver(FrameProcessor)}.
 *
 * See {@link #activate(FrameProcessor)}, {@link #reactivate()}, and {@link FrameProcessor}
 * for flow control implementation.
 *
 * Five frame decoders currently exist, one used for each connection depending on flags and messaging version:
 * 1. {@link FrameDecoderCrc}:
          no compression; payload is protected by CRC32
 * 2. {@link FrameDecoderLZ4}:
          LZ4 compression with custom frame format; payload is protected by CRC32
 * 3. {@link FrameDecoderUnprotected}:
          no compression; no integrity protection
 */
public abstract class FrameDecoder extends ChannelInboundHandlerAdapter
{
    private static final FrameProcessor NO_PROCESSOR =
        frame -> { throw new IllegalStateException("Frame processor invoked on an unregistered FrameDecoder"); };

    private static final FrameProcessor CLOSED_PROCESSOR =
        frame -> { throw new IllegalStateException("Frame processor invoked on a closed FrameDecoder"); };

    public interface FrameProcessor
    {
        /**
         * Frame processor that the frames should be handed off to.
         *
         * @return true if more frames can be taken by the processor, false if the decoder should pause until
         * it's explicitly resumed.
         */
        boolean process(Frame frame) throws IOException;
    }

    public abstract static class Frame
    {
        public final boolean isSelfContained;
        public final int frameSize;

        Frame(boolean isSelfContained, int frameSize)
        {
            this.isSelfContained = isSelfContained;
            this.frameSize = frameSize;
        }

        abstract void release();
        abstract boolean isConsumed();
    }

    /**
     * The payload bytes of a complete frame, i.e. a frame stripped of its headers and trailers,
     * with any verification supported by the protocol confirmed.
     *
     * If {@code isSelfContained} the payload contains one or more {@link Message}, all of which
     * may be parsed entirely from the bytes provided.  Otherwise, only a part of exactly one
     * {@link Message} is contained in the payload; it can be relied upon that this partial {@link Message}
     * will only be delivered in its own unique {@link Frame}.
     */
    public final static class IntactFrame extends Frame
    {
        public final ShareableBytes contents;

        IntactFrame(boolean isSelfContained, ShareableBytes contents)
        {
            super(isSelfContained, contents.remaining());
            this.contents = contents;
        }

        void release()
        {
            contents.release();
        }

        boolean isConsumed()
        {
            return !contents.hasRemaining();
        }

        public void consume()
        {
            contents.consume();
        }
    }

    /**
     * A corrupted frame was encountered; this represents the knowledge we have about this frame,
     * and whether or not the stream is recoverable.
     *
     * Generally we consider a frame with corrupted header as unrecoverable, and frames with intact header,
     * but corrupted payload - as recoverable, since we know and can skip payload size.
     *
     * {@link InboundMessageHandler} further has its own idea of which frames are and aren't recoverable.
     * A recoverable {@link CorruptFrame} can be considered unrecoverable by {@link InboundMessageHandler}
     * if it's the first frame of a large message (isn't self contained).
     */
    public final static class CorruptFrame extends Frame
    {
        public final int readCRC;
        public final int computedCRC;

        CorruptFrame(boolean isSelfContained, int frameSize, int readCRC, int computedCRC)
        {
            super(isSelfContained, frameSize);
            this.readCRC = readCRC;
            this.computedCRC = computedCRC;
        }

        static CorruptFrame recoverable(boolean isSelfContained, int frameSize, int readCRC, int computedCRC)
        {
            return new CorruptFrame(isSelfContained, frameSize, readCRC, computedCRC);
        }

        static CorruptFrame unrecoverable(int readCRC, int computedCRC)
        {
            return new CorruptFrame(false, Integer.MIN_VALUE, readCRC, computedCRC);
        }

        public boolean isRecoverable()
        {
            return frameSize != Integer.MIN_VALUE;
        }

        void release() { }

        boolean isConsumed()
        {
            return true;
        }
    }

    protected final BufferPoolAllocator allocator;

    @VisibleForTesting
    final Deque<Frame> frames = new ArrayDeque<>(4);
    ByteBuffer stash;

    private boolean isActive;
    private boolean isClosed;
    private ChannelHandlerContext ctx;
    private FrameProcessor processor = NO_PROCESSOR;

    FrameDecoder(BufferPoolAllocator allocator)
    {
        this.allocator = allocator;
    }

    abstract void decode(Collection<Frame> into, ShareableBytes bytes);
    abstract void addLastTo(ChannelPipeline pipeline);

    /**
     * @return true if we are actively decoding and processing frames
     */
    public boolean isActive()
    {
        return isActive;
    }
    
    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to start receiving frames.
     */
    public void activate(FrameProcessor processor)
    {
        if (this.processor != NO_PROCESSOR)
            throw new IllegalStateException("Attempted to activate an already active FrameDecoder");

        this.processor = processor;

        isActive = true;
        ctx.read();
    }

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to resume
     * receiving frames after previously indicating that processing should be paused.
     */
    public void reactivate() throws IOException
    {
        if (isActive)
            throw new IllegalStateException("Tried to reactivate an already active FrameDecoder");

        if (deliver(processor))
        {
            isActive = true;
            onExhausted();
        }
    }

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to resume
     * receiving frames after previously indicating that processing should be paused.
     *
     * Does not reactivate processing or reading from the wire, but permits processing as many frames (or parts thereof)
     * that are already waiting as the processor requires.
     */
    void processBacklog(FrameProcessor processor) throws IOException
    {
        deliver(processor);
    }

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to permanently
     * stop receiving frames, e.g. because of an exception caught.
     */
    public void discard()
    {
        isActive = false;
        processor = CLOSED_PROCESSOR;
        if (stash != null)
        {
            ByteBuffer bytes = stash;
            stash = null;
            allocator.put(bytes);
        }
        while (!frames.isEmpty())
            frames.poll().release();
    }

    /**
     * Called by Netty pipeline when a new message arrives; we anticipate in normal operation
     * this will receive messages of type {@link BufferPoolAllocator.Wrapped} or
     * {@link BufferPoolAllocator.Wrapped}.
     *
     * These buffers are unwrapped and passed to {@link #decode(Collection, ShareableBytes)},
     * which collects decoded frames into {@link #frames}, which we send upstream in {@link #deliver}
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException
    {
        if (msg instanceof BufferPoolAllocator.Wrapped)
        {
            ByteBuffer buf = ((BufferPoolAllocator.Wrapped) msg).adopt();
            // netty will probably have mis-predicted the space needed
            allocator.putUnusedPortion(buf);
            channelRead(ShareableBytes.wrap(buf));
        }
        else
        {
            throw new IllegalArgumentException();
        }
    }

    void channelRead(ShareableBytes bytes) throws IOException
    {
        decode(frames, bytes);

        if (isActive)
            isActive = deliver(processor);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx)
    {
        if (isActive)
            onExhausted();
    }

    /**
     * Only to be invoked when frames.isEmpty().
     *
     * If we have been closed, we will now propagate up the channelInactive notification,
     * and otherwise we will ask the channel for more data.
     */
    private void onExhausted()
    {
        if (isClosed)
            close();
        else
            ctx.read();
    }

    /**
     * Deliver any waiting frames, including those that were incompletely read last time, to the provided processor
     * until the processor returns {@code false}, or we finish the backlog.
     *
     * Propagate the final return value of the processor.
     */
    private boolean deliver(FrameProcessor processor) throws IOException
    {
        boolean deliver = true;
        while (deliver && !frames.isEmpty())
        {
            Frame frame = frames.peek();
            deliver = processor.process(frame);

            assert !deliver || frame.isConsumed();
            if (deliver || frame.isConsumed())
            {
                frames.poll();
                frame.release();
            }
        }
        return deliver;
    }

    void stash(ShareableBytes in, int stashLength, int begin, int length)
    {
        ByteBuffer out = allocator.getAtLeast(stashLength);
        copyBytes(in.get(), begin, out, 0, length);
        out.position(length);
        stash = out;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        this.ctx = ctx;
        ctx.channel().config().setAutoRead(false);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        isClosed = true;
        if (frames.isEmpty())
            close();
    }

    private void close()
    {
        discard();
        ctx.fireChannelInactive();
        allocator.release();
    }

    /**
     * Utility: fill {@code out} from {@code in} up to {@code toOutPosition},
     * updating the position of both buffers with the result
     * @return true if there were sufficient bytes to fill to {@code toOutPosition}
     */
    static boolean copyToSize(ByteBuffer in, ByteBuffer out, int toOutPosition)
    {
        int bytesToSize = toOutPosition - out.position();
        if (bytesToSize <= 0)
            return true;

        if (bytesToSize > in.remaining())
        {
            out.put(in);
            return false;
        }

        copyBytes(in, in.position(), out, out.position(), bytesToSize);
        in.position(in.position() + bytesToSize);
        out.position(toOutPosition);
        return true;
    }

    /**
     * @return {@code in} if has sufficient capacity, otherwise
     *         a replacement from {@code BufferPool} that {@code in} is copied into
     */
    ByteBuffer ensureCapacity(ByteBuffer in, int capacity)
    {
        if (in.capacity() >= capacity)
            return in;

        ByteBuffer out = allocator.getAtLeast(capacity);
        in.flip();
        out.put(in);
        allocator.put(in);
        return out;
    }
}
