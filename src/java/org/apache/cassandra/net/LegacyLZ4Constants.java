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

abstract class LegacyLZ4Constants
{
    static final int XXHASH_SEED = 0x9747B28C;

    static final int HEADER_LENGTH = 8  // magic number
                                   + 1  // token
                                   + 4  // compressed length
                                   + 4  // uncompressed length
                                   + 4; // checksum

    static final long MAGIC_NUMBER = (long) 'L' << 56
                                   | (long) 'Z' << 48
                                   | (long) '4' << 40
                                   | (long) 'B' << 32
                                   |        'l' << 24
                                   |        'o' << 16
                                   |        'c' <<  8
                                   |        'k';

    // offsets of header fields
    static final int MAGIC_NUMBER_OFFSET        = 0;
    static final int TOKEN_OFFSET               = 8;
    static final int COMPRESSED_LENGTH_OFFSET   = 9;
    static final int UNCOMPRESSED_LENGTH_OFFSET = 13;
    static final int CHECKSUM_OFFSET            = 17;

    static final int DEFAULT_BLOCK_LENGTH = 1 << 15; // 32 KiB
    static final int MAX_BLOCK_LENGTH     = 1 << 25; // 32 MiB

    static final int BLOCK_TYPE_NON_COMPRESSED = 0x10;
    static final int BLOCK_TYPE_COMPRESSED     = 0x20;

    // xxhash to Checksum adapter discards most significant nibble of value ¯\_(ツ)_/¯
    static final int XXHASH_MASK = 0xFFFFFFF;
}
