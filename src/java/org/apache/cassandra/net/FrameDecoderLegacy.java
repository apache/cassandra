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

import java.nio.ByteBuffer;
import java.util.Collection;

import io.netty.channel.ChannelPipeline;

import static java.lang.Math.max;
import static org.apache.cassandra.net.OutboundConnections.LARGE_MESSAGE_THRESHOLD;

/**
 * {@link InboundMessageHandler} operates on frames that adhere to a certain contract
 * (see {@link FrameDecoder.IntactFrame} and {@link FrameDecoder.CorruptFrame} javadoc).
 *
 * Legacy (pre-4.0) messaging protocol does not natively support framing, however. The job
 * of {@link FrameDecoderLegacy} is turn a raw stream of messages, serialized back to back,
 * into a sequence of frames that adhere to 4.0+ conventions.
 */
class FrameDecoderLegacy extends FrameDecoder
{
    private final int messagingVersion;

    private int remainingBytesInLargeMessage = 0;

    FrameDecoderLegacy(BufferPoolAllocator allocator, int messagingVersion)
    {
        super(allocator);
        this.messagingVersion = messagingVersion;
    }

    final void decode(Collection<Frame> into, ShareableBytes newBytes)
    {
        ByteBuffer in = newBytes.get();
        try
        {
            if (stash != null)
            {
                int length = Message.serializer.inferMessageSize(stash, 0, stash.position(), messagingVersion);
                while (length < 0)
                {
                    if (!in.hasRemaining())
                        return;

                    if (stash.position() == stash.capacity())
                        stash = ensureCapacity(stash, stash.capacity() * 2);
                    copyToSize(in, stash, stash.capacity());

                    length = Message.serializer.inferMessageSize(stash, 0, stash.position(), messagingVersion);
                    if (length >= 0 && length < stash.position())
                    {
                        int excess = stash.position() - length;
                        in.position(in.position() - excess);
                        stash.position(length);
                    }
                }

                final boolean isSelfContained;
                if (length <= LARGE_MESSAGE_THRESHOLD)
                {
                    isSelfContained = true;

                    if (length > stash.capacity())
                        stash = ensureCapacity(stash, length);

                    stash.limit(length);
                    allocator.putUnusedPortion(stash); // we may be over capacity from earlier doubling
                    if (!copyToSize(in, stash, length))
                        return;
                }
                else
                {
                    isSelfContained = false;
                    remainingBytesInLargeMessage = length - stash.position();

                    stash.limit(stash.position());
                    allocator.putUnusedPortion(stash);
                }

                stash.flip();
                assert !isSelfContained || stash.limit() == length;
                ShareableBytes stashed = ShareableBytes.wrap(stash);
                into.add(new IntactFrame(isSelfContained, stashed));
                stash = null;
            }

            if (remainingBytesInLargeMessage > 0)
            {
                if (remainingBytesInLargeMessage >= newBytes.remaining())
                {
                    remainingBytesInLargeMessage -= newBytes.remaining();
                    into.add(new IntactFrame(false, newBytes.sliceAndConsume(newBytes.remaining())));
                    return;
                }
                else
                {
                    Frame frame = new IntactFrame(false, newBytes.sliceAndConsume(remainingBytesInLargeMessage));
                    remainingBytesInLargeMessage = 0;
                    into.add(frame);
                }
            }

            // we loop incrementing our end pointer until we have no more complete messages,
            // at which point we slice the complete messages, and stash the remainder
            int begin = in.position();
            int end = begin;
            int limit = in.limit();

            if (begin == limit)
                return;

            while (true)
            {
                int length = Message.serializer.inferMessageSize(in, end, limit, messagingVersion);

                if (length >= 0)
                {
                    if (end + length <= limit)
                    {
                        // we have a complete message, so just bump our end pointer
                        end += length;

                        // if we have more bytes, continue to look for another message
                        if (end < limit)
                            continue;

                        // otherwise reset length, as we have accounted for it in end
                        length = 0;
                    }
                }

                // we are done; if we have found any complete messages, slice them all into a single frame
                if (begin < end)
                    into.add(new IntactFrame(true, newBytes.slice(begin, end)));

                // now consider stashing anything leftover
                if (length < 0)
                {
                    stash(newBytes, max(64, limit - end), end, limit - end);
                }
                else if (length > LARGE_MESSAGE_THRESHOLD)
                {
                    remainingBytesInLargeMessage = length - (limit - end);
                    Frame frame = new IntactFrame(false, newBytes.slice(end, limit));
                    into.add(frame);
                }
                else if (length > 0)
                {
                    stash(newBytes, length, end, limit - end);
                }
                break;
            }
        }
        catch (Message.InvalidLegacyProtocolMagic e)
        {
            into.add(CorruptFrame.unrecoverable(e.read, Message.PROTOCOL_MAGIC));
        }
        finally
        {
            newBytes.release();
        }
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameDecoderNone", this);
    }
}
