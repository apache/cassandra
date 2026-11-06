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
package org.apache.cassandra.service.paxos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.CRC32C;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.StorageCompatibilityMode;

/**
 * Serialization format for the partition update stored in the system.paxos proposal and
 * most_recent_commit columns. The PartitionUpdate is stored first, followed by the ballot it was
 * proposed or committed under and a fixed 9-byte footer holding the PU messaging version protected
 * by a salted CRC-32C.
 * <p/>
 * The blob holds the PartitionUpdate, the ballot and the messaging version, so the three always
 * belong to the same write. The system.paxos version and ballot columns are separate cells. Two
 * writes with the same timestamp resolve each cell independently, so a column can belong to a
 * different write than the blob. Readers take the messaging version and the ballot from the blob.
 * Writes still fill the version column, so older releases can deserialize the leading
 * PartitionUpdate. The column version is a hint: it only picks the starting messaging version for
 * a blob that has no valid footer.
 * <p/>
 * Support for the raw PartitionUpdate encoding can be dropped once no supported release can leave
 * a raw PartitionUpdate in system.paxos. That requires every supported release to write this
 * format, and paxos state written in the raw encoding to have been repaired or purged (especially
 * if paxos_state_purging=repaired).
 * <p/>
 * Serialization layout:
 *   [N bytes: serialized PartitionUpdate at pu_messaging_version]
 *   [16B: ballot, msb then lsb]
 *   [1B (unsigned): pu_messaging_version]
 *   [4B footer word: format_version(4b) | quantized_size(28b)]
 *   [4B footer: CRC-32C]
 * <p/>
 * The PartitionUpdate is written first, at offset 0. A non-empty update stays readable by a build
 * that takes the version from the version column and ignores trailing bytes. An empty update
 * written with the empty encoding is not readable that way. Readers try the footer first: the blob
 * must be long enough, carry the footer format nibble, have a quantized size that matches its
 * length, and pass the salted CRC-32C. A blob that fails any of these checks is read as a raw
 * PartitionUpdate, because a raw PartitionUpdate can end in bytes that look like a footer. The
 * version column is the starting point for that read. If both reads fail, the blob is corrupt.
 * quantized_size = totalSize >>> 4. The CRC is salted with row_key + cf_id to bind the blob to
 * its row identity. The ballot is left out of the salt: it is inside the blob, so it always
 * belongs to the update beside it. If pu_messaging_version is EMPTY_UPDATE_MESSAGING_VERSION (0)
 * the empty PartitionUpdate is reconstructed on read. The messaging version takes one unsigned
 * byte, so the format version must be bumped to accommodate a messaging version above 255.
 */
public final class PaxosPartitionUpdateBlob
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosPartitionUpdateBlob.class);

    public static final int FOOTER_FORMAT_VERSION = 2;
    private static final int CRC_SIZE = 4;
    private static final int BALLOT_SIZE = 16;
    // footer = 1B pu_messaging_version + 4B packed(format|quantizedSize) + 4B CRC-32C
    static final int FOOTER_SIZE = 9;
    private static final int MAX_LOG_ROW_KEY_HEX_BYTES = 128;

    // sentinel messaging version to avoid serializing empty partition updates that could
    // be reconstructed. Salting of the CRC gives confidence the update is for the correct
    // partition even though the blob does not serialize the row_key/cfid.
    public static final int EMPTY_UPDATE_MESSAGING_VERSION = 0;
    // Sentinel meaning the version column cell is absent/tombstoned. Decode of a blob with a footer
    // ignores the column, this only selects the version used by the raw PartitionUpdate fallback
    // path.
    public static final int VERSION_ABSENT = -1;

    // Fallback messaging versions in reverse order - most likely system.paxos was written with more recent versions
    public static final List<MessagingService.Version> FALLBACK_MESSAGING_VERSIONS =
        MessagingService.Version.supportedVersions().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toUnmodifiableList());

    private PaxosPartitionUpdateBlob() {}

    // allow the same footer unwrapping and fallback mechanism to be used for decoding
    // partition updates and just checking if they are empty for repair.
    @FunctionalInterface
    public interface PartitionUpdateBlobDecoder<T>
    {
        T decode(ByteBuffer buf, int messagingVersion, ByteBuffer rowKey, UUID cfId);
    }

    /**
     * An update and the ballot stored beside it. The ballot is null for a blob written without the
     * footer.
     */
    public static final class Decoded
    {
        public final PartitionUpdate update;
        public final @Nullable Ballot ballot;

        Decoded(PartitionUpdate update, @Nullable Ballot ballot)
        {
            this.update = update;
            this.ballot = ballot;
        }
    }

    /**
     * Encode a PartitionUpdate with the footer format. An empty update is written without bytes
     * when {@link #permitEmptyEncoding} allows it for the messaging version.
     *
     * @param update the partition update to serialize
     * @param messagingVersion the messaging version to use for PU serialization
     * @param rowKey the system.paxos partition key (user table's decorated key bytes)
     * @param cfId the table UUID from system.paxos clustering column
     * @param ballot the ballot (from proposal_ballot or most_recent_commit_at)
     * @return a ByteBuffer containing the serialized update with the ballot and a trailing footer
     */
    public static ByteBuffer encode(PartitionUpdate update, int messagingVersion,
                                    ByteBuffer rowKey, UUID cfId, Ballot ballot)
    {
        return encode(update, messagingVersion, rowKey, cfId, ballot, permitEmptyEncoding(messagingVersion));
    }

    private static boolean permitEmptyEncoding(int messagingVersion)
    {
        return permitEmptyEncoding(messagingVersion, StorageCompatibilityMode.current());
    }

    @VisibleForTesting
    static boolean permitEmptyEncoding(int messagingVersion, StorageCompatibilityMode compatibilityMode)
    {
        // A reader that takes the messaging version from the version column cannot parse an update
        // with no bytes. A build that lacks this class and still takes the version from the
        // version column cannot read the 25-byte blob: its isEmpty runs out of bytes and its
        // fromBytes reads the ballot bytes as a table id. Every storage compatibility mode except
        // NONE keeps such a build as a rollback target. The empty encoding is written only at
        // messaging version 14 or higher and with storage compatibility mode NONE.
        return messagingVersion >= MessagingService.VERSION_60 && compatibilityMode.disabled();
    }

    public static ByteBuffer encode(PartitionUpdate update, int messagingVersion,
                                    ByteBuffer rowKey, UUID cfId, Ballot ballot, boolean permitEmptyPartitionEncoding)
    {
        boolean empty = update.isEmpty();
        boolean useSentinel = empty && permitEmptyPartitionEncoding;
        // Sentinel empties carry no PU bytes and a version of 0; otherwise the PU (possibly an empty
        // PU) is serialized normally at messagingVersion.
        int puMessagingVersion = useSentinel ? EMPTY_UPDATE_MESSAGING_VERSION : messagingVersion;

        // The size estimate only sets the initial capacity. The footer takes its size from the bytes
        // written, so an estimate that disagrees with serialize costs one regrow and cannot corrupt
        // the footer.
        long estimateLong = (useSentinel ? 0 : PartitionUpdate.serializer.serializedSize(update, messagingVersion))
                            + BALLOT_SIZE + FOOTER_SIZE;
        try (DataOutputBuffer out = estimateLong > Integer.MAX_VALUE ? new DataOutputBuffer()
                                                                      : new DataOutputBuffer((int) estimateLong))
        {
            if (!useSentinel)
                PartitionUpdate.serializer.serialize(update, out, messagingVersion);
            out.writeLong(ballot.msb());
            out.writeLong(ballot.lsb());
            out.writeByte(puMessagingVersion); // unsigned byte messaging version

            // The footer word (4 bytes) and the CRC (4 bytes) follow.
            long totalSizeLong = out.position() + Integer.BYTES + CRC_SIZE;
            // Guard against int overflow before casting/packing. Over 2GB will have issues in a cell.
            if (totalSizeLong > Integer.MAX_VALUE)
                throw new IllegalStateException("paxos partition update too large to encode: " + totalSizeLong + " bytes");
            int totalSize = (int) totalSizeLong;
            int quantizedSize = totalSize >>> 4;
            int footerWord = (FOOTER_FORMAT_VERSION << 28) | (quantizedSize & 0x0FFFFFFF);
            out.writeInt(footerWord);

            // CRC over salt + all bytes written so far (PU + ballot + version + footer word). out.buffer()
            // is a flipped duplicate over [0, total-CRC_SIZE); it does not disturb out's position.
            CRC32C crc = new CRC32C();
            feedSalt(crc, rowKey, cfId);
            crc.update(out.buffer());
            out.writeInt((int) crc.getValue());

            return out.asNewBuffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to encode paxos partition update blob", e);
        }
    }

    /**
     * Return true if the encoded partition update is empty (or absent), gracefully handling
     * incorrect messaging versions for the partition update across upgrade boundary
     * @param blob bytes from the proposal or most_recent_commit column, null and zero length are
     *     empty
     * @param columnVersion messaging version from the version column, or VERSION_ABSENT if the cell
     *     is absent; used only as the starting point for a blob written without the footer
     * @param rowKey raw bytes for the row key
     * @param cfId UUID for the table id
     * @return true if the contents of the partition update are empty
     */
    public static boolean updateIsEmpty(@Nullable ByteBuffer blob, int columnVersion, ByteBuffer rowKey, UUID cfId)
    {
        if (blob == null || !blob.hasRemaining())
            return true;
        return decodeInternal(blob, columnVersion, rowKey, cfId, PaxosPartitionUpdateBlob::decodeUpdateIsEmpty).left;
    }

    /**
     * Return the PartitionUpdate encoded in the blob and the ballot stored beside it, gracefully
     * handling incorrect messaging versions for the blob across the upgrade boundary
     * @param blob bytes from the proposal or most_recent_commit column
     * @param columnVersion messaging version from the version column, or VERSION_ABSENT if the cell
     *     is absent; used only as the starting point for a blob written without the footer
     * @param rowKey raw bytes for the row key
     * @param rowTableId UUID for the table id
     * @return the PartitionUpdate and its ballot, which is null for a blob written
     *     without the footer
     * @throws MarshalException if the blob is corrupt, or belongs to another partition or table
     */
    public static Decoded decode(ByteBuffer blob, int columnVersion, ByteBuffer rowKey, UUID rowTableId)
    {
        Pair<PartitionUpdate, Ballot> decoded = decodeInternal(blob, columnVersion, rowKey, rowTableId, PaxosPartitionUpdateBlob::decodePartitionUpdate);
        return new Decoded(decoded.left, decoded.right);
    }

    /**
     * The internal decode that handles fallback for blobs written without the footer.
     * @param blob bytes from the proposal or most_recent_commit column
     * @param columnVersion messaging version from the version column, or VERSION_ABSENT if the cell
     *     is absent
     * @param rowKey raw bytes for the row key
     * @param rowTableId UUID for the table id
     * @param puDecoder actual PartitionUpdate blob decoder
     * @return the decoded value paired with the ballot from the blob, or null for a blob written
     *     without the footer
     * @param <T> return type from the embedded PartitionUpdate blob decode
     */
    private static <T> Pair<T, Ballot> decodeInternal(ByteBuffer blob, int columnVersion,
                                                      ByteBuffer rowKey, UUID rowTableId,
                                                      PartitionUpdateBlobDecoder<T> puDecoder)
    {
        try
        {
            try
            {
                // A footer that matches the format nibble and the quantized size is only a likely
                // signature, because a raw PartitionUpdate can end in bytes that look like a footer.
                // A failed footer or CRC check therefore falls back to the raw PartitionUpdate path.
                // The fallback can be removed, making a footer match without a valid CRC a hard
                // error, once no supported release can leave a raw PartitionUpdate in system.paxos.
                // That holds when every supported release writes the footer format and the paxos
                // state written in the raw encoding has been repaired or purged.
                return decodeWithFooter(blob.duplicate(), rowKey, rowTableId, puDecoder);
            }
            catch (RuntimeException | AssertionError footerFailure)
            {
                Pair<T, Integer> fallback;
                try
                {
                    fallback = decodeWithoutFooterAndFallback(blob.duplicate(), columnVersion, rowKey,
                                                              rowTableId, puDecoder);
                }
                catch (RuntimeException | AssertionError fallbackFailure)
                {
                    fallbackFailure.addSuppressed(footerFailure);
                    throw fallbackFailure;
                }
                // A raw PartitionUpdate that ends in footer-like bytes with a CRC mismatch also logs this warning.
                if (footerFailure instanceof FooterCrcMismatchException)
                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, "crc-mismatch-" + rowTableId, 1, TimeUnit.MINUTES,
                                     "paxos blob failed its footer CRC but decoded as a raw partition update; " +
                                     "row_key={} cf_id={}: {}",
                                     rowKeyHex(rowKey), rowTableId, footerFailure.getMessage());
                if (columnVersion != VERSION_ABSENT && !fallback.right.equals(columnVersion))
                    logger.debug("paxos partition update decoded at messaging version v{} not the column version v{}; " +
                                 "row_key={} cf_id={}",
                                 fallback.right, columnVersion, rowKeyHex(rowKey), rowTableId);
                return Pair.create(fallback.left, null);
            }
        }
        catch (RuntimeException | AssertionError tr)
        {
            String message = String.format("Failed to decode paxos partition update; row_key=%s cf_id=%s",
                                           rowKeyHex(rowKey), rowTableId);
            NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, rowTableId.toString(), 1, TimeUnit.MINUTES,
                             "{}: {}", message, describeWithSuppressed(tr));
            throw new MarshalException(message, tr);
        }
    }

    /**
     * Names a failure and the message of every failure suppressed under it, so a log line shows
     * both the raw decode failure and the footer failure that preceded it, such as a CRC mismatch.
     */
    private static String describeWithSuppressed(Throwable failure)
    {
        StringBuilder description = new StringBuilder().append(failure);
        for (Throwable suppressed : failure.getSuppressed())
        {
            String text = suppressed.getMessage() != null ? suppressed.getMessage() : suppressed.toString();
            description.append("; also failed: ").append(text);
        }
        return description.toString();
    }

    /**
     * Decode a blob with a footer back into a PartitionUpdate, validating from the tail.
     * <p/>
     * Checks the blob holds the 9-byte footer, that the quantized size equals
     * {@code totalSize >>> 4} and that the format nibble is the footer format. It then checks
     * the blob is long enough for the ballot and the footer, and that the salted CRC-32C matches. A
     * blob that fails any of these is not the footer format, for example a raw PartitionUpdate, and
     * the method throws so that {@link #decodeInternal} can fall back to the raw path.
     *
     * @throws MarshalException / RuntimeException on any footer validation or PU decode failure
     */
    private static <T> Pair<T, Ballot> decodeWithFooter(ByteBuffer buf, ByteBuffer rowKey, UUID rowTableId, PartitionUpdateBlobDecoder<T> puDecoder)
    {
        int totalSize = buf.remaining();
        int startPosition = buf.position();

        if (totalSize < FOOTER_SIZE)
            throw new MarshalException("paxos footer too small for decode (" + totalSize + " bytes)");

        int footerStart = startPosition + totalSize - FOOTER_SIZE;
        int footerWord = buf.getInt(footerStart + 1);
        int footerFormatVersion = (footerWord >>> 28) & 0xF;
        int quantizedSize = footerWord & 0x0FFFFFFF;
        if ((totalSize >>> 4) != quantizedSize)
            throw new MarshalException(String.format("paxos footer quantized_size mismatch (footer=%d, actual=%d)",
                                                     quantizedSize, totalSize >>> 4));

        if (footerFormatVersion != FOOTER_FORMAT_VERSION)
            throw new MarshalException(String.format("paxos footer format version unknown 0x%X", footerFormatVersion));

        if (totalSize < FOOTER_SIZE + BALLOT_SIZE)
            throw new MarshalException("paxos blob too small for decode (" + totalSize + " bytes)");

        checkCrc(buf, rowKey, rowTableId);

        int puMessagingVersion = buf.get(footerStart) & 0xFF;
        int ballotStart = footerStart - BALLOT_SIZE;
        Ballot ballot = Ballot.fromBytes(buf.getLong(ballotStart), buf.getLong(ballotStart + 8));
        return Pair.create(decodeLeadingUpdate(buf, ballotStart, puMessagingVersion, rowKey, rowTableId, puDecoder), ballot);
    }

    /**
     * Check the trailing CRC-32C of a blob with a footer. The CRC covers the salt, then every byte
     * before the last four.
     *
     * @throws MarshalException if the computed CRC differs from the stored one
     */
    private static void checkCrc(ByteBuffer buf, ByteBuffer rowKey, UUID cfId)
    {
        int startPosition = buf.position();
        int crcLen = buf.remaining() - CRC_SIZE;
        CRC32C crc = new CRC32C();
        feedSalt(crc, rowKey, cfId);
        ByteBuffer crcInput = buf.duplicate();
        crcInput.position(startPosition).limit(startPosition + crcLen);
        crc.update(crcInput);
        int computedCRC = (int) crc.getValue();
        int storedCRC = buf.getInt(startPosition + crcLen);
        if (computedCRC != storedCRC)
            throw new FooterCrcMismatchException(String.format(
                "paxos footer CRC-32C mismatch: computed=0x%08X stored=0x%08X", computedCRC, storedCRC));
    }

    /** Marks a footer whose format nibble and size matched but whose CRC did not. */
    private static final class FooterCrcMismatchException extends MarshalException
    {
        FooterCrcMismatchException(String message)
        {
            super(message);
        }
    }

    /**
     * Decode the PartitionUpdate at the start of a blob. The update spans from the buffer position
     * to the absolute index {@code end}.
     */
    private static <T> T decodeLeadingUpdate(ByteBuffer buf, int end, int puMessagingVersion,
                                             ByteBuffer rowKey, UUID cfId,
                                             PartitionUpdateBlobDecoder<T> puDecoder)
    {
        ByteBuffer puBuf = buf.duplicate();
        puBuf.position(buf.position()).limit(end);
        return puDecoder.decode(puBuf, puMessagingVersion, rowKey, cfId);
    }

    private static PartitionUpdate puFromBytesAndAssertMatches(ByteBuffer blob, int messagingVersion, ByteBuffer rowKey, UUID rowTableId)
    {
        PartitionUpdate pu;
        try
        {
            // A raw update must fill the buffer. A damaged blob that falls back to this path can
            // still parse, and the bytes after the update are then the ballot, version and footer.
            DataInputBuffer in = new DataInputBuffer(blob, true);
            pu = PartitionUpdate.serializer.deserialize(in, messagingVersion, DeserializationHelper.Flag.LOCAL);
            if (in.available() > 0)
                throw new MarshalException(in.available() + " bytes remain after the partition update");
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        if (pu != null)
        {
            UUID puTableId = pu.metadata().id.asUUID();
            if (!rowTableId.equals(puTableId))
                throw new IllegalStateException(String.format("paxos.state row tableId %s does not match partition update tableId %s",
                                                              rowTableId, puTableId));
            ByteBuffer puPartitionKey = pu.partitionKey().getKey();
            if (ByteBufferUtil.compareUnsigned(rowKey, puPartitionKey) != 0)
                throw new IllegalStateException(String.format("partition update partition key %s does not match row key",
                                                              rowKeyHex(puPartitionKey)));
        }
        return pu;
    }

    /**
     * Decode a raw PartitionUpdate blob written without the footer.
     * Tries supported versions in descending order (v14, v13, v12), capped at the column's claimed
     * version (v12 when the column is absent); first success wins. Logging is left to the caller.
     *
     * @return the decoded value paired with the messaging version that succeeded
     */
    private static <T> Pair<T, Integer> decodeWithoutFooterAndFallback(ByteBuffer blob, int messagingVersion,
                                                                         ByteBuffer rowKey, UUID rowTableId,
                                                                         PartitionUpdateBlobDecoder<T> puDecoder)
    {
        // Cap fallback at the column's claimed version; never up-parse above it (v13 vs v12 differ
        // in TTL-overflow/uint-timestamp semantics that the tableId/row-key check would not catch,
        // and v14 adds the TCM epoch). Absent column caps at v12. Floor at minimum_version so a
        // torn, corrupt or pre-4.0 version cell (< v12) attempts v12 and at least one attempt
        // always runs.
        int claimed = (messagingVersion == VERSION_ABSENT) ? MessagingService.VERSION_40 : messagingVersion;
        int upperBound = Math.max(MessagingService.minimum_version, claimed);

        Throwable firstFailure = null;
        for (MessagingService.Version attempt : FALLBACK_MESSAGING_VERSIONS) // descending: v14, v13, v12
        {
            int attemptVersion = attempt.value;
            if (attemptVersion > upperBound) // never up-parse above the column's claimed version
                continue;
            try
            {
                return Pair.create(puDecoder.decode(blob.duplicate(), attemptVersion, rowKey, rowTableId), attemptVersion);
            }
            catch (RuntimeException | AssertionError e)
            {
                if (firstFailure == null) firstFailure = e;
                else firstFailure.addSuppressed(e);
            }
        }
        // upperBound >= minimum_version guarantees at least one attempt ran; guard defensively so a
        // future change can never throw a bare NPE here.
        if (firstFailure == null)
            throw new MarshalException("paxos legacy fallback made no decode attempt (upperBound=" + upperBound + ')');
        if (firstFailure instanceof AssertionError) throw (AssertionError) firstFailure;
        throw (RuntimeException) firstFailure;
    }

    @VisibleForTesting
    static boolean decodeUpdateIsEmpty(ByteBuffer buf, int messagingVersion, ByteBuffer rowKey, UUID cfId)
    {
        if (messagingVersion == EMPTY_UPDATE_MESSAGING_VERSION)
        {
            if (buf.hasRemaining())
                throw new MarshalException("no partition update expected but " + buf.remaining() + " bytes remaining");
            else
                return true;
        }
        else
        {
            try
            {
                return PartitionUpdate.PartitionUpdateSerializer.isEmpty(buf, rowKey, cfId, messagingVersion);
            }
            catch (IOException ioe)
            {
                throw new RuntimeException(ioe);
            }
        }
    }

    @VisibleForTesting
    static PartitionUpdate decodePartitionUpdate(ByteBuffer buf, int messagingVersion, ByteBuffer rowKey, UUID cfId)
    {
        if (messagingVersion == EMPTY_UPDATE_MESSAGING_VERSION) // empty partition updates need reconstruction
        {
            if (buf.hasRemaining())
                throw new MarshalException("paxos footer does not expect bytes for empty partitions");
            try
            {
                TableMetadata tableMetadata = Schema.instance.getExistingTableMetadata(TableId.fromUUID(cfId));
                DecoratedKey dk = tableMetadata.partitioner.decorateKey(rowKey);
                return PartitionUpdate.emptyUpdate(tableMetadata, dk);
            }
            catch (UnknownTableException ute) // perhaps table dropped since original encode
            {
                throw new RuntimeException("Unknown table exception", ute);
            }
        }
        else // decode the partition update and validate the rowKey/cfId match
        {
            return puFromBytesAndAssertMatches(buf, messagingVersion, rowKey, cfId);
        }
    }

    private static void feedSalt(CRC32C crc, ByteBuffer rowKey, UUID cfId)
    {
        // row_key bytes
        ByteBuffer rkDup = rowKey.duplicate();
        crc.update(rkDup);

        // cf_id as 16 bytes (MSB then LSB, little-endian)
        long msb = cfId.getMostSignificantBits();
        long lsb = cfId.getLeastSignificantBits();
        updateLong(crc, msb);
        updateLong(crc, lsb);
    }

    static void updateLong(CRC32C crc, long v)
    {
        crc.update((int)(v        ) & 0xFF);
        crc.update((int)(v >>>  8) & 0xFF);
        crc.update((int)(v >>> 16) & 0xFF);
        crc.update((int)(v >>> 24) & 0xFF);
        crc.update((int)(v >>> 32) & 0xFF);
        crc.update((int)(v >>> 40) & 0xFF);
        crc.update((int)(v >>> 48) & 0xFF);
        crc.update((int)(v >>> 56) & 0xFF);
    }

    private static String rowKeyHex(ByteBuffer rowKey)
    {
        if (rowKey == null)
            return "<null>";
        int length = rowKey.remaining();
        if (length <= MAX_LOG_ROW_KEY_HEX_BYTES)
            return ByteBufferUtil.bytesToHex(rowKey);
        ByteBuffer truncated = rowKey.duplicate();
        truncated.limit(truncated.position() + MAX_LOG_ROW_KEY_HEX_BYTES);
        return ByteBufferUtil.bytesToHex(truncated) + "...";
    }
}
