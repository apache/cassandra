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

/**
 * <h2>File transfer</h2>
 *
 * When tranferring whole or subsections of an sstable, only the DATA component is shipped. To that end,
 * there are three "modes" of an sstable transfer that need to be handled somewhat differently:
 *
 * 1) uncompressed sstable - data needs to be read into user space so it can be manipulated: checksum validation,
 * apply stream compression (see next section), and/or TLS encryption.
 *
 * 2) compressed sstable, transferred with SSL/TLS - data needs to be read into user space as that is where the TLS encryption
 * needs to happen. Netty does not allow the pretense of doing zero-copy transfers when TLS is in the pipeline;
 * data must explicitly be pulled into user-space memory for TLS encryption to work.
 *
 * 3) compressed sstable, transferred without SSL/TLS - data can be streamed via zero-copy transfer as the data does not
 * need to be manipulated (it can be sent "as-is").
 *
 * <h3>Compressing the data</h3>
 * We always want to transfer as few bytes as possible of the wire when streaming a file. If the
 * sstable is not already compressed via table compression options, we apply an on-the-fly stream compression
 * to the data. The stream compression format is documented in
 * {@link org.apache.cassandra.streaming.async.StreamCompressionSerializer}
 *
 * You may be wondering: why implement your own compression scheme? why not use netty's built-in compression codecs,
 * like {@link io.netty.handler.codec.compression.Lz4FrameEncoder}? That makes complete sense if all the sstables
 * to be streamed are non using sstable compression (and obviously you wouldn't use stream compression when the sstables
 * are using sstable compression). The problem is when you have a mix of files, some using sstable compression
 * and some not. You can either:
 *
 * - send the files of one type over one kind of socket, and the others over another socket
 * - send them both over the same socket, but then auto-adjust per each file type.
 *
 * I've opted for the latter to keep socket/channel management simpler and cleaner.
 *
 */
package org.apache.cassandra.db.streaming;
