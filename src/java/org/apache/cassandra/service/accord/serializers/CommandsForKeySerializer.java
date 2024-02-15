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

package org.apache.cassandra.service.accord.serializers;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.primitives.Ints;

import accord.api.Key;
import accord.impl.CommandsForKey;
import accord.impl.CommandsForKey.Info;
import accord.impl.CommandsForKey.InternalStatus;
import accord.impl.CommandsForKey.NoInfo;
import accord.local.Node;
import accord.primitives.Routable.Domain;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.vint.VIntCoding;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.ArrayBuffers.cachedInts;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer.TxnIdFlags.EXTENDED;
import static org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer.TxnIdFlags.EXTENDED_BITS;
import static org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer.TxnIdFlags.RAW;
import static org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer.TxnIdFlags.RAW_BITS;
import static org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer.TxnIdFlags.STANDARD;
import static org.apache.cassandra.utils.ByteBufferUtil.readLeastSignificantBytes;
import static org.apache.cassandra.utils.ByteBufferUtil.writeLeastSignificantBytes;
import static org.apache.cassandra.utils.ByteBufferUtil.writeMostSignificantBytes;

public class CommandsForKeySerializer
{
    private static final int HAS_MISSING_DEPS_HEADER_BIT = 0x1;
    private static final int HAS_EXECUTE_AT_HEADER_BIT = 0x2;
    private static final int HAS_NON_STANDARD_FLAGS = 0x4;

    /**
     * We read/write a fixed number of intial bytes for each command, with an initial flexible number of flag bits
     * and the remainder interpreted as the HLC/epoch/node.
     *
     * The preamble encodes:
     *  vint32: number of commands
     *  vint32: number of unique node Ids
     *  [unique node ids]
     *  two flag bytes:
     *   bit 0 is set if there are any missing ids;
     *   bit 1 is set if there are any executeAt specified
     *   bit 2 is set if there are any queries present besides reads/writes
     *   bits 3-4 number of header bytes to read for each command
     *   bits 5-6: level 0 extra hlc bytes to read
     *   bits 7-8: level 1 extra hlc bytes to read (+ 1 + level 0)
     *   bits 9-10: level 2 extra hlc bytes to read (+ 1 + level 1)
     *   bits 12-13: level 3 extra hlc bytes to read (+ 1 + level 2)
     *
     * In order, for each command, we consume:
     * 3 bits for the InternalStatus of the command
     * 1 optional bit: if the status encodes an executeAt, indicating if the executeAt is not the TxnId
     * 1 optional bit: if the status encodes any dependencies and there are non-zero missing ids, indicating if there are any missing for this command
     * 1 or 2 bits for the kind of the TxnId: 0=key read, 1=key write, 2=exclusive sync point,3=read 16 bits
     * 1 bit encoding if the epoch has changed
     * 2 optional bits: if the prior bit is set, indicating how many bits should be read for the epoch increment: 0=none (increment by 1); 1=4, 2=8, 3=32
     * 4 option bits: if prior bits=01, epoch delta
     * N node id bits (where 2^N unique node ids in the CFK)
     * 2 bits indicating how many more payload bytes should be read, with mapping written in header
     * all remaining bits are interpreted as a delta from the prior HLC
     *
     * if txnId kind flag is 3, read an additional 2 bytes for TxnId flag
     * if epoch increment flag is 2 or 3, read additional 1 or 4 bytes for epoch delta
     * if executeAt is expected, read vint32 for epoch, vint32 for delta from txnId hlc, and ceil(N/8) bytes for node id
     *
     * After writing all transactions, we then write out the missing txnid collections. This is written at the end
     * so that on deserialization we have already read all of the TxnId. This also permits more efficient serialization,
     * as we can encode a single bit stream with the optimal number of bits.
     * TODO (desired): we could prefix this collection with the subset of TxnId that are actually missing from any other
     *   deps, so as to shrink this collection much further.
     */
    // TODO (expected): offer filtering option that does not need to reconstruct objects/info, reusing prior encoding decisions
    // TODO (expected): accept new redundantBefore on load to avoid deserializing stale data
    // TODO (desired): determine timestamp resolution as a factor of 10
    public static ByteBuffer toBytesWithoutKey(CommandsForKey cfk)
    {
        int commandCount = cfk.size();
        if (commandCount == 0)
        {
            // TODO (expected): we should not need to special-case, but best solution here is not to store redundantBefore;
            //    but this requires some modest deeper changes, so for now special-case serialization when empty
            Timestamp redundantBefore = cfk.redundantBefore();
            ByteBuffer out = ByteBuffer.allocate(TypeSizes.sizeofUnsignedVInt(0) +
                                                 TypeSizes.sizeofUnsignedVInt(redundantBefore.epoch()) +
                                                 TypeSizes.sizeofUnsignedVInt(redundantBefore.hlc()) +
                                                 TypeSizes.sizeofUnsignedVInt(redundantBefore.flags()) +
                                                 TypeSizes.sizeofUnsignedVInt(redundantBefore.node.id));
            VIntCoding.writeUnsignedVInt32(0, out);
            VIntCoding.writeUnsignedVInt(redundantBefore.epoch(), out);
            VIntCoding.writeUnsignedVInt(redundantBefore.hlc(), out);
            VIntCoding.writeUnsignedVInt32(redundantBefore.flags(), out);
            VIntCoding.writeUnsignedVInt32(redundantBefore.node.id, out);
            out.flip();
            return out;
        }

        int[] nodeIds = cachedInts().getInts(Math.min(64, commandCount));
        try
        {
            // first compute the unique Node Ids and some basic characteristics of the data, such as
            // whether we have any missing transactions to encode, any executeAt that are not equal to their TxnId
            // and whether there are any non-standard flag bits to encode
            boolean hasNonStandardFlags = false;
            int nodeIdCount, missingIdCount = 0, executeAtCount = 0, bitsPerExecuteAtFlags = 0;
            int bitsPerExecuteAtEpochDelta = 0, bitsPerExecuteAtHlcDelta = 1; // to permit us to use full 64 bits and encode in 5 bits we force at least one hlc bit
            {
                nodeIdCount = 1;
                nodeIds[0] = cfk.redundantBefore().node.id;
                for (int i = 0 ; i < commandCount ; ++i)
                {
                    if (nodeIdCount + 1 >= nodeIds.length)
                    {
                        nodeIdCount = compact(nodeIds);
                        if (nodeIdCount > nodeIds.length/2)
                            nodeIds = cachedInts().resize(nodeIds, nodeIds.length, nodeIds.length * 2);
                    }

                    TxnId txnId = cfk.txnId(i);
                    Info info = cfk.info(i);

                    hasNonStandardFlags |= txnIdFlags(txnId) != STANDARD;
                    nodeIds[nodeIdCount++] = txnId.node.id;

                    if (info.getClass() == NoInfo.class)
                        continue;

                    missingIdCount += info.missing.length;

                    if (info.executeAt == txnId)
                        continue;

                    nodeIds[nodeIdCount++] = info.executeAt.node.id;
                    bitsPerExecuteAtEpochDelta = Math.max(bitsPerExecuteAtEpochDelta, numberOfBitsToRepresent(info.executeAt.epoch() - txnId.epoch()));
                    bitsPerExecuteAtHlcDelta = Math.max(bitsPerExecuteAtHlcDelta, numberOfBitsToRepresent(info.executeAt.hlc() - txnId.hlc()));
                    bitsPerExecuteAtFlags = Math.max(bitsPerExecuteAtFlags, numberOfBitsToRepresent(info.executeAt.flags()));
                    executeAtCount += 1;
                }
                nodeIdCount = compact(nodeIds);
                Invariants.checkState(nodeIdCount > 0);
            }

            // We can now use this information to calculate the fixed header size, compute the amount
            // of additional space we'll need to store the TxnId and its basic info
            int bitsPerNodeId = numberOfBitsToRepresent(nodeIdCount);
            int minHeaderBits = 7 + bitsPerNodeId + (hasNonStandardFlags ? 1 : 0);
            int infoHeaderBits = (executeAtCount > 0 ? 1 : 0) + (missingIdCount > 0 ? 1 : 0);
            int maxHeaderBits = minHeaderBits;
            int totalBytes = 0;

            long prevEpoch = cfk.redundantBefore().epoch();
            long prevHlc = cfk.redundantBefore().hlc();
            int[] bytesHistogram = cachedInts().getInts(12);
            Arrays.fill(bytesHistogram, 0);
            for (int i = 0 ; i < commandCount ; ++i)
            {
                int headerBits = minHeaderBits;
                int payloadBits = 0;

                TxnId txnId = cfk.txnId(i);
                {
                    long epoch = txnId.epoch();
                    Invariants.checkState(epoch >= prevEpoch);
                    long epochDelta = epoch - prevEpoch;
                    long hlc = txnId.hlc();
                    long hlcDelta = hlc - prevHlc;

                    if (epochDelta > 0)
                    {
                        if (hlcDelta < 0)
                            hlcDelta = -1 - hlcDelta;

                        headerBits += 3;
                        if (epochDelta > 1)
                        {
                            if (epochDelta <= 0xf) headerBits += 4;
                            else if (epochDelta <= 0xff) totalBytes += 1;
                            else { totalBytes += 4; Invariants.checkState(epochDelta <= 0xffffffffL); }
                        }
                    }

                    payloadBits += numberOfBitsToRepresent(hlcDelta);
                    prevEpoch = epoch;
                    prevHlc = hlc;
                }

                if (hasNonStandardFlags && txnIdFlags(txnId) == RAW)
                    totalBytes += 2;

                Info info = cfk.info(i);
                if (info.status.hasInfo)
                    headerBits += infoHeaderBits;
                maxHeaderBits = Math.max(headerBits, maxHeaderBits);
                int basicBytes = (headerBits + payloadBits + 7)/8;
                bytesHistogram[basicBytes]++;
            }

            int minBasicBytes = -1, maxBasicBytes = 0;
            for (int i = 0 ; i < bytesHistogram.length ; ++i)
            {
                if (bytesHistogram[i] == 0) continue;
                if (minBasicBytes == -1) minBasicBytes = i;
                maxBasicBytes = i;
            }
            for (int i = minBasicBytes + 1 ; i <= maxBasicBytes ; ++i)
                bytesHistogram[i] += bytesHistogram[i-1];

            int flags = (missingIdCount > 0 ? HAS_MISSING_DEPS_HEADER_BIT : 0)
                        | (executeAtCount > 0 ? HAS_EXECUTE_AT_HEADER_BIT : 0)
                        | (hasNonStandardFlags ? HAS_NON_STANDARD_FLAGS : 0);

            int headerBytes = (maxHeaderBits+7)/8;
            flags |= Invariants.checkArgument(headerBytes - 1, headerBytes <= 4) << 3;

            int hlcBytesLookup;
            {   // 2bits per size, first value may be zero and remainder may be increments of 1-4;
                // only need to be able to encode a distribution of approx. 8 bytes at most, so
                // pick lowest number we need first, then next lowest as 25th %ile while ensuring value of 1-4;
                // then pick highest number we need, ensuring at least 2 greater than second (leaving room for third)
                // then pick third number as 75th %ile, but at least 1 less than highest, and one more than second
                // finally, ensure third then second are distributed so that there is no more than a gap of 4 between them and the next
                int l0 = Math.max(0, Math.min(3, minBasicBytes - headerBytes));
                int l1 = Math.max(l0+1, Math.min(l0+4,Arrays.binarySearch(bytesHistogram, commandCount/4) - headerBytes));
                int l3 = Math.max(l1+2, maxBasicBytes - headerBytes);
                int l2 = Math.max(l1+1, Math.min(l3-1, Arrays.binarySearch(bytesHistogram, (3*commandCount)/4) - headerBytes));
                while (l3-l2 > 4) ++l2;
                while (l2-l1 > 4) ++l1;
                hlcBytesLookup = setHlcBytes(l0, l1, l2, l3);
                flags |= (l0 | ((l1-(1+l0))<<2) | ((l2-(1+l1))<<4) | ((l3-(1+l2))<<6)) << 5;
            }
            int hlcFlagLookup = hlcBytesLookupToHlcFlagLookup(hlcBytesLookup);

            totalBytes += bytesHistogram[minBasicBytes] * (headerBytes + getHlcBytes(hlcBytesLookup, getHlcFlag(hlcFlagLookup, minBasicBytes - headerBytes)));
            for (int i = minBasicBytes + 1 ; i <= maxBasicBytes ; ++i)
                totalBytes += (bytesHistogram[i] - bytesHistogram[i-1]) * (headerBytes + getHlcBytes(hlcBytesLookup, getHlcFlag(hlcFlagLookup, i - headerBytes)));
            totalBytes += TypeSizes.sizeofUnsignedVInt(commandCount);
            totalBytes += TypeSizes.sizeofUnsignedVInt(nodeIdCount);
            totalBytes += TypeSizes.sizeofUnsignedVInt(nodeIds[0]);
            for (int i = 1 ; i < nodeIdCount ; ++i)
                totalBytes += TypeSizes.sizeofUnsignedVInt(nodeIds[i] - nodeIds[i-1]);
            totalBytes += 2;

            Arrays.fill(bytesHistogram, minBasicBytes, maxBasicBytes + 1, 0);
            cachedInts().forceDiscard(bytesHistogram);

            prevEpoch = cfk.redundantBefore().epoch();
            prevHlc = cfk.redundantBefore().hlc();
            // account for encoding redundantBefore
            totalBytes += TypeSizes.sizeofUnsignedVInt(prevEpoch);
            totalBytes += TypeSizes.sizeofUnsignedVInt(prevHlc);
            totalBytes += 2; // flags TODO (expected): pack this along with uniqueIdBits, as usually zero bits should be needed
            totalBytes += (bitsPerNodeId+7)/8;

            if (missingIdCount + executeAtCount > 0)
            {
                // account for encoding missing id stream
                int missingIdBits = 1 + numberOfBitsToRepresent(commandCount);
                int executeAtBits = bitsPerNodeId
                                    + bitsPerExecuteAtEpochDelta
                                    + bitsPerExecuteAtHlcDelta
                                    + bitsPerExecuteAtFlags;
                totalBytes += (missingIdBits * missingIdCount + executeAtBits * executeAtCount + 7)/8;
                if (executeAtCount > 0)
                    totalBytes += 2;
            }

            ByteBuffer out = ByteBuffer.allocate(totalBytes);
            VIntCoding.writeUnsignedVInt32(commandCount, out);
            VIntCoding.writeUnsignedVInt32(nodeIdCount, out);
            VIntCoding.writeUnsignedVInt32(nodeIds[0], out);
            for (int i = 1 ; i < nodeIdCount ; ++i) // TODO (desired): can encode more efficiently as a stream of N bit integers
                VIntCoding.writeUnsignedVInt32(nodeIds[i] - nodeIds[i-1], out);
            out.putShort((short)flags);

            VIntCoding.writeUnsignedVInt(prevEpoch, out);
            VIntCoding.writeUnsignedVInt(prevHlc, out);
            out.putShort((short) cfk.redundantBefore().flags());
            writeLeastSignificantBytes(Arrays.binarySearch(nodeIds, 0, nodeIdCount, cfk.redundantBefore().node.id), (bitsPerNodeId+7)/8, out);

            int executeAtMask = executeAtCount > 0 ? 1 : 0;
            int missingDepsMask = missingIdCount > 0 ? 1 : 0;
            int flagsIncrement = hasNonStandardFlags ? 2 : 1;
            // TODO (desired): check this loop compiles correctly to only branch on epoch case, for binarySearch and flushing
            for (int i = 0 ; i < commandCount ; ++i)
            {
                TxnId txnId = cfk.txnId(i);
                Info info = cfk.info(i);
                InternalStatus status = info.status;

                long bits = status.ordinal();
                int bitIndex = 3;

                int statusHasInfo = status.hasInfo ? 1 : 0;
                long hasExecuteAt = info.executeAt != null & info.executeAt != txnId ? 1 : 0;
                bits |= hasExecuteAt << bitIndex;
                bitIndex += statusHasInfo & executeAtMask;

                long hasMissingIds = info.missing != CommandsForKey.NO_TXNIDS ? 1 : 0;
                bits |= hasMissingIds << bitIndex;
                bitIndex += statusHasInfo & missingDepsMask;

                long flagBits = txnIdFlagsBits(txnId);
                boolean writeFullFlags = flagBits == RAW_BITS;
                bits |= flagBits << bitIndex;
                bitIndex += flagsIncrement;

                long hlcBits;
                int extraEpochDeltaBytes = 0;
                {
                    long epoch = txnId.epoch();
                    long delta = epoch - prevEpoch;
                    long hlc = txnId.hlc();
                    hlcBits = hlc - prevHlc;
                    if (delta == 0)
                    {
                        bitIndex++;
                    }
                    else
                    {
                        bits |= 1L << bitIndex++;
                        if (hlcBits < 0)
                        {
                            hlcBits = -1 - hlcBits;
                            bits |= 1L << bitIndex;
                        }
                        bitIndex++;
                        if (delta > 1)
                        {
                            if (delta <= 0xf)
                            {
                                bits |= 1L << bitIndex;
                                bits |= delta << (bitIndex + 2);
                                bitIndex += 4;
                            }
                            else
                            {
                                bits |= (delta <= 0xff ? 2L : 3L) << bitIndex;
                                extraEpochDeltaBytes = Ints.checkedCast(delta);
                            }
                        }
                        bitIndex += 2;
                    }
                    prevEpoch = epoch;
                    prevHlc = hlc;
                }

                bits |= ((long)Arrays.binarySearch(nodeIds, 0, nodeIdCount, txnId.node.id)) << bitIndex;
                bitIndex += bitsPerNodeId;

                bits |= hlcBits << (bitIndex + 2);
                hlcBits >>>= 8*headerBytes - (bitIndex + 2);
                int hlcFlag = getHlcFlag(hlcFlagLookup, (7 + numberOfBitsToRepresent(hlcBits))/8);
                bits |= ((long)hlcFlag) << bitIndex;

                writeLeastSignificantBytes(bits, headerBytes, out);
                writeLeastSignificantBytes(hlcBits, getHlcBytes(hlcBytesLookup, hlcFlag), out);

                if (writeFullFlags)
                    out.putShort((short)txnId.flags());

                if (extraEpochDeltaBytes > 0)
                {
                    if (extraEpochDeltaBytes <= 0xff) out.put((byte)extraEpochDeltaBytes);
                    else out.putInt(extraEpochDeltaBytes);
                }
            }

            if ((executeAtCount | missingIdCount) > 0)
            {
                int bitsPerCommandId =  numberOfBitsToRepresent(commandCount);
                int bitsPerMissingId = 1 + bitsPerCommandId;
                int bitsPerExecuteAt = bitsPerExecuteAtEpochDelta + bitsPerExecuteAtHlcDelta + bitsPerExecuteAtFlags + bitsPerNodeId;
                Invariants.checkState(bitsPerExecuteAtEpochDelta < 64);
                Invariants.checkState(bitsPerExecuteAtHlcDelta <= 64);
                Invariants.checkState(bitsPerExecuteAtFlags <= 16);
                if (executeAtMask > 0) // we encode both 15 and 16 bits for flag length as 15 to fit in a short
                    out.putShort((short) ((bitsPerExecuteAtEpochDelta << 10) | ((bitsPerExecuteAtHlcDelta-1) << 4) | (Math.min(15, bitsPerExecuteAtFlags))));
                long buffer = 0L;
                int bufferCount = 0;

                for (int i = 0 ; i < commandCount ; ++i)
                {
                    Info info = cfk.info(i);
                    if (info.getClass() == NoInfo.class)
                        continue;

                    TxnId txnId = cfk.txnId(i);
                    if (info.executeAt != txnId)
                    {
                        Timestamp executeAt = info.executeAt;
                        int nodeIdx = Arrays.binarySearch(nodeIds, 0, nodeIdCount, executeAt.node.id);
                        if (bitsPerExecuteAt <= 64)
                        {
                            Invariants.checkState(executeAt.epoch() >= txnId.epoch());
                            long executeAtBits = executeAt.epoch() - txnId.epoch();
                            int offset = bitsPerExecuteAtEpochDelta;
                            executeAtBits |= (executeAt.hlc() - txnId.hlc()) << offset ;
                            offset += bitsPerExecuteAtHlcDelta;
                            executeAtBits |= ((long)executeAt.flags()) << offset;
                            offset += bitsPerExecuteAtFlags;
                            executeAtBits |= ((long)nodeIdx) << offset;
                            buffer = flushBits(buffer, bufferCount, executeAtBits, bitsPerExecuteAt, out);
                            bufferCount = (bufferCount + bitsPerExecuteAt) & 63;
                        }
                        else
                        {
                            buffer = flushBits(buffer, bufferCount, executeAt.epoch() - txnId.epoch(), bitsPerExecuteAtEpochDelta, out);
                            bufferCount = (bufferCount + bitsPerExecuteAtEpochDelta) & 63;
                            buffer = flushBits(buffer, bufferCount, executeAt.hlc() - txnId.hlc(), bitsPerExecuteAtHlcDelta, out);
                            bufferCount = (bufferCount + bitsPerExecuteAtHlcDelta) & 63;
                            buffer = flushBits(buffer, bufferCount, executeAt.flags(), bitsPerExecuteAtFlags, out);
                            bufferCount = (bufferCount + bitsPerExecuteAtFlags) & 63;
                            buffer = flushBits(buffer, bufferCount, nodeIdx, bitsPerNodeId, out);
                            bufferCount = (bufferCount + bitsPerNodeId) & 63;
                        }
                    }

                    if (info.missing.length > 0)
                    {
                        int j = 0;
                        while (j < info.missing.length - 1)
                        {
                            int missingId = cfk.indexOf(info.missing[j++]);
                            buffer = flushBits(buffer, bufferCount, missingId, bitsPerMissingId, out);
                            bufferCount = (bufferCount + bitsPerMissingId) & 63;
                        }
                        int missingId = cfk.indexOf(info.missing[info.missing.length - 1]);
                        missingId |= 1L << bitsPerCommandId;
                        buffer = flushBits(buffer, bufferCount, missingId, bitsPerMissingId, out);
                        bufferCount = (bufferCount + bitsPerMissingId) & 63;
                    }
                }

                writeMostSignificantBytes(buffer, (bufferCount + 7)/8, out);
            }

            out.flip();
            return out;
        }
        finally
        {
            cachedInts().forceDiscard(nodeIds);
        }
    }

    private static long flushBits(long buffer, int bufferCount, long add, int addCount, ByteBuffer out)
    {
        Invariants.checkArgument(addCount == 64 || 0 == (add & (-1L << addCount)));
        int total = bufferCount + addCount;
        if (total < 64)
        {
            return buffer | (add << 64 - total);
        }
        else
        {
            buffer |= add >>> total - 64;
            out.putLong(buffer);
            return total == 64 ? 0 : (add << (128 - total));
        }
    }

    public static CommandsForKey fromBytes(Key key, ByteBuffer in)
    {
        if (!in.hasRemaining())
            return null;

        in = in.duplicate();
        int commandCount = VIntCoding.readUnsignedVInt32(in);
        if (commandCount == 0)
        {
            long epoch = VIntCoding.readUnsignedVInt(in);
            long hlc = VIntCoding.readUnsignedVInt(in);
            int flags = VIntCoding.readUnsignedVInt32(in);
            Node.Id id = new Node.Id(VIntCoding.readUnsignedVInt32(in));
            return new CommandsForKey(key).withoutRedundant(TxnId.fromValues(epoch, hlc, flags, id));
        }

        TxnId[] txnIds = new TxnId[commandCount];
        Info[] infos = new Info[commandCount];
        int nodeIdCount = VIntCoding.readUnsignedVInt32(in);
        int bitsPerNodeId = numberOfBitsToRepresent(nodeIdCount);
        long nodeIdMask = (1L << bitsPerNodeId) - 1;
        Node.Id[] nodeIds = new Node.Id[nodeIdCount]; // TODO (expected): use a shared reusable scratch buffer
        {
            int prev = VIntCoding.readUnsignedVInt32(in);
            nodeIds[0] = new Node.Id(prev);
            for (int i = 1 ; i < nodeIdCount ; ++i)
                nodeIds[i] = new Node.Id(prev += VIntCoding.readUnsignedVInt32(in));
        }

        int missingDepsMasks, executeAtMasks, txnIdFlagsMask;
        int headerByteCount, hlcBytesLookup;
        {
            int flags = in.getShort();
            missingDepsMasks = 0 != (flags & HAS_MISSING_DEPS_HEADER_BIT) ? 1 : 0;
            executeAtMasks = 0 != (flags & HAS_EXECUTE_AT_HEADER_BIT) ? 1 : 0;
            txnIdFlagsMask = 0 != (flags & HAS_NON_STANDARD_FLAGS) ? 3 : 1;
            headerByteCount = 1 + ((flags >>> 3) & 0x3);
            hlcBytesLookup = setHlcByteDeltas((flags >>> 5) & 0x3, (flags >>> 7) & 0x3, (flags >>> 9) & 0x3, (flags >>> 11) & 0x3);
        }

        long prevEpoch = VIntCoding.readUnsignedVInt32(in);
        long prevHlc = VIntCoding.readUnsignedVInt32(in);
        TxnId redundantBefore = TxnId.fromValues(prevEpoch, prevHlc, in.getShort(),
                                                 nodeIds[(int)readLeastSignificantBytes((bitsPerNodeId+7)/8, in)]);


        for (int i = 0 ; i < commandCount ; ++i)
        {
            long header = readLeastSignificantBytes(headerByteCount, in);
            header |= 1L << (8 * headerByteCount); // marker so we know where to shift-left most-significant bytes to
            InternalStatus status = InternalStatus.get((int) (header & 0x7));
            header >>>= 3;

            int executeAtInfoOffset, missingDepsInfoOffset;
            {
                int infoMask = status.hasInfo ? 1 : 0;
                int executeAtMask = infoMask & executeAtMasks, missingDepsMask = infoMask & missingDepsMasks;
                executeAtInfoOffset = ((int)header & executeAtMask) << 1;
                header >>>= executeAtMask;
                missingDepsInfoOffset = (int)header & missingDepsMask;
                header >>>= missingDepsMask;
            }

            Txn.Kind kind = TXN_ID_FLAG_BITS_KIND_LOOKUP[((int)header & txnIdFlagsMask)];
            header >>>= Integer.bitCount(txnIdFlagsMask);

            boolean hlcIsNegative = false;
            long epoch = prevEpoch;
            int readEpochBytes = 0;
            {
                boolean hasEpochDelta = (header & 1) == 1;
                header >>>= 1;
                if (hasEpochDelta)
                {
                    hlcIsNegative = (header & 1) == 1;
                    header >>>= 1;

                    int epochFlag = ((int)header & 0x3);
                    header >>>= 2;
                    switch (epochFlag)
                    {
                        default: throw new AssertionError("Unexpected value not 0-3");
                        case 0: ++epoch; break;
                        case 1: epoch += (header & 0xf); header >>>= 4; break;
                        case 2: readEpochBytes = 1; break;
                        case 3: readEpochBytes = 4; break;
                    }
                }
            }

            Node.Id node = nodeIds[(int)(header & nodeIdMask)];
            header >>>= bitsPerNodeId;

            int readHlcBytes = getHlcBytes(hlcBytesLookup, (int)(header & 0x3));
            header >>>= 2;

            long hlc = header;
            {
                long highestBit = Long.highestOneBit(hlc);
                hlc ^= highestBit;
                int hlcShift = Long.numberOfTrailingZeros(highestBit);
                hlc |= readLeastSignificantBytes(readHlcBytes, in) << hlcShift;
            }
            if (hlcIsNegative)
                hlc = -1-hlc;
            hlc += prevHlc;

            int flags = kind != null ? 0 : in.getShort();
            if (readEpochBytes > 0)
                epoch += readEpochBytes == 1 ? (in.get() & 0xff) : in.getInt();

            TxnId txnId = kind != null ? new TxnId(epoch, hlc, kind, Domain.Key, node)
                                       : TxnId.fromValues(epoch, hlc, flags, node);

            txnIds[i] = txnId;
            infos[i] = DECODE_INFOS[(executeAtInfoOffset | missingDepsInfoOffset)*STATUS_COUNT + status.ordinal()];

            prevEpoch = epoch;
            prevHlc = hlc;
        }

        if (executeAtMasks + missingDepsMasks > 0)
        {
            TxnId[] missingIdBuffer = cachedTxnIds().get(8);
            int missingIdCount = 0, maxIdBufferCount = 0;
            int bitsPerTxnId = numberOfBitsToRepresent(commandCount);
            int txnIdMask = (1 << bitsPerTxnId) - 1;
            int bitsPerMissingId = bitsPerTxnId + 1;

            int decodeBits = executeAtMasks > 0 ? in.getShort() & 0xffff : 0;
            int bitsPerEpochDelta = decodeBits >>> 10;
            int bitsPerHlcDelta = 1 + ((decodeBits >>> 4) & 0x3f);
            int bitsPerFlags = decodeBits & 0xf;
            if (bitsPerFlags == 15) bitsPerFlags = 16;
            int bitsPerExecuteAt = bitsPerEpochDelta + bitsPerHlcDelta + bitsPerFlags + bitsPerNodeId;

            long epochDeltaMask = bitsPerEpochDelta == 0 ? 0 : (-1L >>> (64 - bitsPerEpochDelta));
            long hlcDeltaMask = (-1L >>> (64 - bitsPerHlcDelta));
            long flagsMask = bitsPerFlags == 0 ? 0 : (-1L >>> (64 - bitsPerFlags));

            final BitReader reader = new BitReader();

            for (int i = 0 ; i < commandCount ; ++i)
            {
                Info info = infos[i];
                if (info.getClass() == NoInfo.class)
                    continue;

                TxnId txnId = txnIds[i];
                Timestamp executeAt = txnId;
                if (info.executeAt == null)
                {
                    long epoch, hlc;
                    int flags;
                    Node.Id id;
                    if (bitsPerExecuteAt <= 64)
                    {
                        long executeAtBits = reader.read(bitsPerExecuteAt, in);
                        epoch = txnId.epoch() + (executeAtBits & epochDeltaMask);
                        executeAtBits >>>= bitsPerEpochDelta;
                        hlc = txnId.hlc() + (executeAtBits & hlcDeltaMask);
                        executeAtBits >>>= bitsPerHlcDelta;
                        flags = (int)(executeAtBits & flagsMask);
                        executeAtBits >>>= bitsPerFlags;
                        id = nodeIds[(int)(executeAtBits & nodeIdMask)];
                    }
                    else
                    {
                        epoch = txnId.epoch() + reader.read(bitsPerEpochDelta, in);
                        hlc = txnId.hlc() + reader.read(bitsPerHlcDelta, in);
                        flags = (int) reader.read(bitsPerFlags, in);
                        id = nodeIds[(int)(reader.read(bitsPerNodeId, in))];
                    }
                    executeAt = Timestamp.fromValues(epoch, hlc, flags, id);
                }

                TxnId[] missing = info.missing;
                if (missing == null)
                {
                    int prev = -1;
                    while (true)
                    {
                        if (missingIdCount == missingIdBuffer.length)
                            missingIdBuffer = cachedTxnIds().resize(missingIdBuffer, missingIdCount, missingIdCount * 2);

                        int next = (int) reader.read(bitsPerMissingId, in);
                        Invariants.checkState(next > prev);
                        missingIdBuffer[missingIdCount++] = txnIds[next & txnIdMask];
                        if (next >= commandCount)
                            break; // finished this array
                        prev = next;
                    }

                    missing = Arrays.copyOf(missingIdBuffer, missingIdCount);
                    maxIdBufferCount = missingIdCount;
                    missingIdCount = 0;
                }

                infos[i] = Info.create(txnId, info.status, executeAt, missing);
            }

            cachedTxnIds().forceDiscard(missingIdBuffer, maxIdBufferCount);
        }

        return CommandsForKey.SerializerSupport.create(key, redundantBefore, txnIds, infos);
    }

    private static int getHlcBytes(int lookup, int index)
    {
        return (lookup >>> (index * 4)) & 0xf;
    }

    private static int setHlcBytes(int value1, int value2, int value3, int value4)
    {
        return value1 | (value2 << 4) | (value3 << 8) | (value4 << 12);
    }

    private static int setHlcByteDeltas(int value1, int value2, int value3, int value4)
    {
        value2 += 1 + value1;
        value3 += 1 + value2;
        value4 += 1 + value3;
        return setHlcBytes(value1, value2, value3, value4);
    }

    private static int getHlcFlag(int flagsLookup, int bytes)
    {
        return (flagsLookup >>> (bytes * 2)) & 0x3;
    }

    private static int hlcBytesLookupToHlcFlagLookup(int bytesLookup)
    {
        int flagsLookup = 0;
        int flagIndex = 0;
        for (int bytesIndex = 0 ; bytesIndex < 4 ; bytesIndex++)
        {
            int flagLimit = getHlcBytes(bytesLookup, bytesIndex);
            while (flagIndex <= flagLimit)
                flagsLookup |= bytesIndex << (2 * flagIndex++);
        }
        return flagsLookup;
    }

    private static int compact(int[] buffer)
    {
        Arrays.sort(buffer);
        int count = 0;
        int j = 0;
        while (j < buffer.length)
        {
            int prev;
            buffer[count++] = prev = buffer[j];
            while (++j < buffer.length && buffer[j] == prev) {}
        }
        return count;
    }

    private static int numberOfBitsToRepresent(long value)
    {
        return 64 - Long.numberOfLeadingZeros(value);
    }

    private static int numberOfBitsToRepresent(int value)
    {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    static final class BitReader
    {
        private long bitBuffer;
        private int bitCount;

        long read(int readCount, ByteBuffer in)
        {
            long result = bitBuffer >>> (64 - readCount);
            int remaining = bitCount - readCount;
            if (remaining >= 0)
            {
                bitBuffer <<= readCount;
                bitCount = remaining;
            }
            else if (in.remaining() >= 8)
            {
                readCount -= bitCount;
                bitBuffer = in.getLong();
                bitCount = 64 - readCount;
                result |= (bitBuffer >>> bitCount);
                bitBuffer <<= readCount;
            }
            else
            {
                readCount -= bitCount;
                while (readCount > 8)
                {
                    long next = in.get() & 0xff;
                    readCount -= 8;
                    result |= next << readCount;
                }
                long next = in.get() & 0xff;
                bitCount = 8 - readCount;
                result |= next >>> bitCount;
                bitBuffer = next << (64 - bitCount);
            }
            return result;
        }
    }

    enum TxnIdFlags
    {
        STANDARD, EXTENDED, RAW;
        static final int EXTENDED_BITS = 0x2;
        static final int RAW_BITS = 0x3;
    }

    private static TxnIdFlags txnIdFlags(TxnId txnId)
    {
        if (txnId.flags() > Timestamp.IDENTITY_FLAGS || txnId.domain() != Domain.Key)
            return RAW;
        switch (txnId.kind())
        {
            default: throw new AssertionError("Unhandled Kind: " + txnId.kind());
            case Read:
            case Write:
                return STANDARD;
            case ExclusiveSyncPoint:
                return EXTENDED;
            case SyncPoint:
            case LocalOnly:
            case EphemeralRead:
                return RAW;
        }
    }

    private static long  txnIdFlagsBits(TxnId txnId)
    {
        switch (txnIdFlags(txnId))
        {
            default: throw new AssertionError("Unhandled TxnIdFlag: " + txnIdFlags(txnId));
            case RAW: return RAW_BITS;
            case EXTENDED: return EXTENDED_BITS;
            case STANDARD:
                return txnId.kind() == Read ? 0 : 1;
        }
    }

    private static final Txn.Kind[] TXN_ID_FLAG_BITS_KIND_LOOKUP = new Txn.Kind[] { Read, Write, ExclusiveSyncPoint, null };
    private static final int STATUS_COUNT = InternalStatus.values().length;
    private static final Info[] DECODE_INFOS = new Info[4 * STATUS_COUNT];
    static
    {
        for (InternalStatus status : InternalStatus.values())
        {
            int ordinal = status.ordinal();
            DECODE_INFOS[ordinal] = status.asNoInfo;
            DECODE_INFOS[STATUS_COUNT+ordinal] = Info.createMock(status, Timestamp.NONE, null);
            DECODE_INFOS[2*STATUS_COUNT+ordinal] = Info.createMock(status, null, CommandsForKey.NO_TXNIDS);
            DECODE_INFOS[3*STATUS_COUNT+ordinal] = Info.createMock(status, null, null);
        }
    }

}
