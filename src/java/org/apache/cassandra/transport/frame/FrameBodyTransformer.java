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

package org.apache.cassandra.transport.frame;

import java.io.IOException;
import java.util.EnumSet;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.Frame;

public interface FrameBodyTransformer
{
    /**
     * Accepts the input buffer representing the frame body of an incoming message and applies a transformation.
     * Example transformations include decompression and recombining checksummed chunks into a single, serialized
     * message body.
     * @param inputBuf the frame body from an inbound message
     * @return the new frame body bytes
     * @throws IOException if the transformation failed for any reason
     */
    ByteBuf transformInbound(ByteBuf inputBuf, EnumSet<Frame.Header.Flag> flags) throws IOException;

    /**
     * Accepts an input buffer representing the frame body of an outbound message and applies a transformation.
     * Example transformations include compression and splitting into checksummed chunks.

     * @param inputBuf the frame body from an outgoing message
     * @return the new frame body bytes
     * @throws IOException if the transformation failed for any reason
     */
    ByteBuf transformOutbound(ByteBuf inputBuf) throws IOException;

    /**
     * Returns an EnumSet of the flags that should be added to the header for any message whose frame body has been
     * modified by the transformer. E.g. it may add perform chunking & checksumming to the frame body,
     * compress it, or both.
     * @return EnumSet containing the header flags to set on messages transformed
     */
    EnumSet<Frame.Header.Flag> getOutboundHeaderFlags();

}
