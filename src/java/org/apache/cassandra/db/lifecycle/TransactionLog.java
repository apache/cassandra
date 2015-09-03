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
package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.util.*;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Runnables;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SnapshotDeletingTask;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.db.Directories.OnTxnErr;
import static org.apache.cassandra.db.Directories.FileType;
import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * IMPORTANT: When this object is involved in a transactional graph, and is not encapsulated in a LifecycleTransaction,
 * for correct behaviour its commit MUST occur before any others, since it may legitimately fail. This is consistent
 * with the Transactional API, which permits one failing action to occur at the beginning of the commit phase, but also
 * *requires* that the prepareToCommit() phase only take actions that can be rolled back.
 *
 * A class that tracks sstable files involved in a transaction across sstables:
 * if the transaction succeeds the old files should be deleted and the new ones kept; vice-versa if it fails.
 *
 * The transaction log file contains new and old sstables as follows:
 *
 * add:[sstable-2][CRC]
 * remove:[sstable-1,max_update_time,num files][CRC]
 *
 * where sstable-2 is a new sstable to be retained if the transaction succeeds and sstable-1 is an old sstable to be
 * removed. CRC is an incremental CRC of the file content up to this point. For old sstable files we also log the
 * last update time of all files for the sstable descriptor and a checksum of vital properties such as update times
 * and file sizes.
 *
 * Upon commit we add a final line to the log file:
 *
 * commit:[commit_time][CRC]
 *
 * When the transaction log is cleaned-up by the TransactionTidier, which happens only after any old sstables have been
 * osoleted, then any sstable files for old sstables are removed before deleting the transaction log if the transaction
 * was committed, vice-versa if the transaction was aborted.
 *
 * On start-up we look for any transaction log files and repeat the cleanup process described above.
 *
 * See CASSANDRA-7066 for full details.
 */
public class TransactionLog extends Transactional.AbstractTransactional implements Transactional
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionLog.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.HOURS);

    /**
     * If the format of the lines in the transaction log is wrong or the checksum
     * does not match, then we throw this exception.
     */
    public static final class CorruptTransactionLogException extends RuntimeException
    {
        public final TransactionFile file;

        public CorruptTransactionLogException(String message, TransactionFile file)
        {
            super(message);
            this.file = file;
        }
    }

    public enum RecordType
    {
        ADD,    // new files to be retained on commit
        REMOVE, // old files to be retained on abort
        COMMIT, // commit flag
        ABORT;  // abort flag
        public static RecordType fromPrefix(String prefix)
        {
            return valueOf(prefix.toUpperCase());
        }
    }

    /**
     * A log file record, each record is encoded in one line and has different
     * content depending on the record type.
     */
    final static class Record
    {
        public final RecordType type;
        public final String relativeFilePath;
        public final long updateTime;
        public final int numFiles;
        public final String record;

        static String REGEX_STR = "^(add|remove|commit|abort):\\[([^,]*),?([^,]*),?([^,]*)\\]$";
        static Pattern REGEX = Pattern.compile(REGEX_STR, Pattern.CASE_INSENSITIVE); // (add|remove|commit|abort):[*,*,*]

        public static Record make(String record, boolean isLast)
        {
            try
            {
                Matcher matcher = REGEX.matcher(record);
                if (!matcher.matches() || matcher.groupCount() != 4)
                    throw new IllegalStateException(String.format("Invalid record \"%s\"", record));

                RecordType type = RecordType.fromPrefix(matcher.group(1));
                return new Record(type, matcher.group(2), Long.valueOf(matcher.group(3)), Integer.valueOf(matcher.group(4)), record);
            }
            catch (Throwable t)
            {
                if (!isLast)
                    throw t;

                int pos = record.indexOf(':');
                if (pos <= 0)
                    throw t;

                RecordType recordType;
                try
                {
                    recordType = RecordType.fromPrefix(record.substring(0, pos));
                }
                catch (Throwable ignore)
                {
                    throw t;
                }

                return new Record(recordType, "", 0, 0, record);

            }
        }

        public static Record makeCommit(long updateTime)
        {
            return new Record(RecordType.COMMIT, "", updateTime, 0, "");
        }

        public static Record makeAbort(long updateTime)
        {
            return new Record(RecordType.ABORT, "", updateTime, 0, "");
        }

        public static Record makeNew(String relativeFilePath)
        {
            return new Record(RecordType.ADD, relativeFilePath, 0, 0, "");
        }

        public static Record makeOld(String parentFolder, String relativeFilePath)
        {
            return makeOld(getTrackedFiles(parentFolder, relativeFilePath), relativeFilePath);
        }

        public static Record makeOld(List<File> files, String relativeFilePath)
        {
            long lastModified = files.stream()
                                     .mapToLong(File::lastModified)
                                     .reduce(0L, Long::max);
            return new Record(RecordType.REMOVE, relativeFilePath, lastModified, files.size(), "");
        }

        private Record(RecordType type,
                       String relativeFilePath,
                       long updateTime,
                       int numFiles,
                       String record)
        {
            this.type = type;
            this.relativeFilePath = hasFilePath(type) ? relativeFilePath : ""; // only meaningful for some records
            this.updateTime = type == RecordType.REMOVE ? updateTime : 0; // only meaningful for old records
            this.numFiles = type == RecordType.REMOVE ? numFiles : 0; // only meaningful for old records
            this.record = record.isEmpty() ? format() : record;
        }

        private static boolean hasFilePath(RecordType type)
        {
            return type == RecordType.ADD || type == RecordType.REMOVE;
        }

        private String format()
        {
            return String.format("%s:[%s,%d,%d]", type.toString(), relativeFilePath, updateTime, numFiles);
        }

        public byte[] getBytes()
        {
            return record.getBytes(FileUtils.CHARSET);
        }

        public boolean verify(String parentFolder, boolean lastRecordIsCorrupt)
        {
            if (type != RecordType.REMOVE)
                return true;

            List<File> files = getTrackedFiles(parentFolder);

            // Paranoid sanity checks: we create another record by looking at the files as they are
            // on disk right now and make sure the information still matches
            Record currentRecord = Record.makeOld(files, relativeFilePath);
            if (updateTime != currentRecord.updateTime && currentRecord.numFiles > 0)
            {
                logger.error("Unexpected files detected for sstable [{}], record [{}]: last update time [{}] should have been [{}]",
                             relativeFilePath,
                             record,
                             new Date(currentRecord.updateTime),
                             new Date(updateTime));
                return false;
            }

            if (lastRecordIsCorrupt && currentRecord.numFiles < numFiles)
            { // if we found a corruption in the last record, then we continue only if the number of files matches exactly.
                logger.error("Unexpected files detected for sstable [{}], record [{}]: number of files [{}] should have been [{}]",
                             relativeFilePath,
                             record,
                             currentRecord.numFiles,
                             numFiles);
                return false;
            }

            return true;
        }

        public List<File> getTrackedFiles(String parentFolder)
        {
            if (!hasFilePath(type))
                return Collections.emptyList();

            return getTrackedFiles(parentFolder, relativeFilePath);
        }

        public static List<File> getTrackedFiles(String parentFolder, String relativeFilePath)
        {
            return Arrays.asList(new File(parentFolder).listFiles((dir, name) -> name.startsWith(relativeFilePath)));
        }

        @Override
        public int hashCode()
        {
            // see comment in equals
            return Objects.hash(type, relativeFilePath);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
                return false;

            if (getClass() != obj.getClass())
                return false;

            final Record other = (Record)obj;

            // we exclude on purpose checksum, update time and count as
            // we don't want duplicated records that differ only by
            // properties that might change on disk, especially COMMIT records,
            // there should be only one regardless of update time
            return type.equals(other.type) &&
                   relativeFilePath.equals(other.relativeFilePath);
        }

        @Override
        public String toString()
        {
            return record;
        }
    }

    /**
     * The transaction log file, which contains many records.
     */
    final static class TransactionFile
    {
        static String EXT = ".log";
        static char SEP = '_';
        // cc_txn_opname_id.log (where cc is one of the sstable versions defined in BigVersion)
        static String FILE_REGEX_STR = String.format("^(.{2})_txn_(.*)_(.*)%s$", EXT);
        static Pattern FILE_REGEX = Pattern.compile(FILE_REGEX_STR);
        static String LINE_REGEX_STR = "^(.*)\\[(\\d*)\\]$"; // *[checksum]
        static Pattern LINE_REGEX = Pattern.compile(LINE_REGEX_STR);

        public final File file;
        public final TransactionData parent;
        public final Set<Record> records = new HashSet<>();
        public final Checksum checksum = new CRC32();

        public TransactionFile(TransactionData parent)
        {
            this.file = new File(parent.getFileName());
            this.parent = parent;
        }

        public void readRecords()
        {
            records.clear();
            checksum.reset();

            Iterator<String> it = FileUtils.readLines(file).iterator();
            while(it.hasNext())
                records.add(readRecord(it.next(), !it.hasNext())); // JLS execution order is left-to-right

            for (Record record : records)
            {
                if (!record.verify(parent.getFolder(), false))
                    throw new CorruptTransactionLogException(String.format("Failed to verify transaction %s record [%s]: unexpected disk state, aborting", parent.getId(), record),
                                                             this);
            }
        }

        private Record readRecord(String line, boolean isLast)
        {
            Matcher matcher = LINE_REGEX.matcher(line);
            if (!matcher.matches() || matcher.groupCount() != 2)
            {
                handleReadRecordError(String.format("cannot parse line \"%s\"", line), isLast);
                return Record.make(line, isLast);
            }

            byte[] bytes = matcher.group(1).getBytes(FileUtils.CHARSET);
            checksum.update(bytes, 0, bytes.length);

            if (checksum.getValue() != Long.valueOf(matcher.group(2)))
                handleReadRecordError(String.format("invalid line checksum %s for \"%s\"", matcher.group(2), line), isLast);

            try
            {
                return Record.make(matcher.group(1), isLast);
            }
            catch (Throwable t)
            {
                throw new CorruptTransactionLogException(String.format("Cannot make record \"%s\": %s", line, t.getMessage()), this);
            }
        }

        private void handleReadRecordError(String message, boolean isLast)
        {
            if (isLast)
            {
                for (Record record : records)
                {
                    if (!record.verify(parent.getFolder(), true))
                        throw new CorruptTransactionLogException(String.format("Last record of transaction %s is corrupt [%s] and at least " +
                                                                               "one previous record does not match state on disk, unexpected disk state, aborting",
                                                                               parent.getId(), message),
                                                                 this);
                }

                // if only the last record is corrupt and all other records have matching files on disk, @see verifyRecord,
                // then we simply exited whilst serializing the last record and we carry on
                logger.warn(String.format("Last record of transaction %s is corrupt or incomplete [%s], but all previous records match state on disk; continuing", parent.getId(), message));

            }
            else
            {
                throw new CorruptTransactionLogException(String.format("Non-last record of transaction %s is corrupt [%s], unexpected disk state, aborting", parent.getId(), message), this);
            }
        }

        public void commit()
        {
            assert !completed() : "Already completed!";
            addRecord(Record.makeCommit(System.currentTimeMillis()));
        }

        public void abort()
        {
            assert !completed() : "Already completed!";
            addRecord(Record.makeAbort(System.currentTimeMillis()));
        }

        public boolean committed()
        {
            return records.contains(Record.makeCommit(0));
        }

        public boolean aborted()
        {
            return records.contains(Record.makeAbort(0));
        }

        public boolean completed()
        {
            return committed() || aborted();
        }

        public boolean add(RecordType type, SSTable table)
        {
            Record record = makeRecord(type, table);
            if (records.contains(record))
                return false;

            addRecord(record);
            return true;
        }

        private Record makeRecord(RecordType type, SSTable table)
        {
            String relativePath = FileUtils.getRelativePath(parent.getFolder(), table.descriptor.baseFilename());
            if (type == RecordType.ADD)
            {
                return Record.makeNew(relativePath);
            }
            else if (type == RecordType.REMOVE)
            {
                return Record.makeOld(parent.getFolder(), relativePath);
            }
            else
            {
                throw new AssertionError("Invalid record type " + type);
            }
        }

        private void addRecord(Record record)
        {
            // we only checksum the records, not the checksums themselves
            byte[] bytes = record.getBytes();
            checksum.update(bytes, 0, bytes.length);

            records.add(record);
            FileUtils.append(file, String.format("%s[%d]", record, checksum.getValue()));

            parent.sync();
        }

        public void remove(RecordType type, SSTable table)
        {
            Record record = makeRecord(type, table);

            assert records.contains(record) : String.format("[%s] is not tracked by %s", record, file);

            records.remove(record);
            deleteRecord(record);
        }

        public boolean contains(RecordType type, SSTable table)
        {
            return records.contains(makeRecord(type, table));
        }

        public void deleteRecords(RecordType type)
        {
            assert file.exists() : String.format("Expected %s to exists", file);
            records.stream()
                   .filter((r) -> r.type == type)
                   .forEach(this::deleteRecord);
            records.clear();
        }

        private void deleteRecord(Record record)
        {
            List<File> files = record.getTrackedFiles(parent.getFolder());
            if (files.isEmpty())
                return; // Files no longer exist, nothing to do

            // we sort the files in ascending update time order so that the last update time
            // stays the same even if we only partially delete files
            files.sort((f1, f2) -> Long.compare(f1.lastModified(), f2.lastModified()));

            files.forEach(TransactionLog::delete);
        }

        public Set<File> getTrackedFiles(RecordType type)
        {
            return records.stream()
                          .filter((r) -> r.type == type)
                          .map((r) -> r.getTrackedFiles(parent.getFolder()))
                          .flatMap(List::stream)
                          .collect(Collectors.toSet());
        }

        public void delete()
        {
            TransactionLog.delete(file);
        }

        public boolean exists()
        {
            return file.exists();
        }

        @Override
        public String toString()
        {
            return FileUtils.getRelativePath(parent.getFolder(), FileUtils.getCanonicalPath(file));
        }
    }

    /**
     * We split the transaction data from TransactionLog that implements the behavior
     * because we need to reconstruct any left-overs and clean them up, as well as work
     * out which files are temporary. So for these cases we don't want the full
     * transactional behavior, plus it's handy for the TransactionTidier.
     */
    final static class TransactionData implements AutoCloseable
    {
        private final OperationType opType;
        private final UUID id;
        private final File folder;
        private final TransactionFile file;
        private int folderDescriptor;

        static TransactionData make(File logFile)
        {
            Matcher matcher = TransactionFile.FILE_REGEX.matcher(logFile.getName());
            assert matcher.matches() && matcher.groupCount() == 3;

            // For now we don't need this but it is there in case we need to change
            // file format later on, the version is the sstable version as defined in BigFormat
            //String version = matcher.group(1);

            OperationType operationType = OperationType.fromFileName(matcher.group(2));
            UUID id = UUID.fromString(matcher.group(3));

            return new TransactionData(operationType, logFile.getParentFile(), id);
        }

        TransactionData(OperationType opType, File folder, UUID id)
        {
            this.opType = opType;
            this.id = id;
            this.folder = folder;
            this.file = new TransactionFile(this);
            this.folderDescriptor = CLibrary.tryOpenDirectory(folder.getPath());
        }

        public Throwable readLogFile(Throwable accumulate)
        {
            try
            {
                file.readRecords();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }

            return accumulate;
        }

        public void close()
        {
            if (folderDescriptor > 0)
            {
                CLibrary.tryCloseFD(folderDescriptor);
                folderDescriptor = -1;
            }
        }

        void sync()
        {
            if (folderDescriptor > 0)
                CLibrary.trySync(folderDescriptor);
        }

        OperationType getType()
        {
            return opType;
        }

        UUID getId()
        {
            return id;
        }

        boolean completed()
        {
            return  file.completed();
        }

        Throwable removeUnfinishedLeftovers(Throwable accumulate)
        {
            try
            {
                if (file.committed())
                    file.deleteRecords(RecordType.REMOVE);
                else
                    file.deleteRecords(RecordType.ADD);

                // we sync the parent file descriptor between contents and log deletion
                // to ensure there is a happens before edge between them
                sync();

                file.delete();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }

            return accumulate;
        }

        Set<File> getTemporaryFiles()
        {
            sync();

            if (!file.exists())
                return Collections.emptySet();

            if (file.committed())
                return file.getTrackedFiles(RecordType.REMOVE);
            else
                return file.getTrackedFiles(RecordType.ADD);
        }

        String getFileName()
        {
            String fileName = StringUtils.join(BigFormat.latestVersion,
                                               TransactionFile.SEP,
                                               "txn",
                                               TransactionFile.SEP,
                                               opType.fileName,
                                               TransactionFile.SEP,
                                               id.toString(),
                                               TransactionFile.EXT);
            return StringUtils.join(folder, File.separator, fileName);
        }

        String getFolder()
        {
            return folder.getPath();
        }

        static boolean isLogFile(String name)
        {
            return TransactionFile.FILE_REGEX.matcher(name).matches();
        }

        @VisibleForTesting
        TransactionFile getLogFile()
        {
            return file;
        }

        @Override
        public String toString()
        {
            return String.format("[%s]", file.toString());
        }
    }

    private final Tracker tracker;
    private final TransactionData data;
    private final Ref<TransactionLog> selfRef;
    // Deleting sstables is tricky because the mmapping might not have been finalized yet,
    // and delete will fail (on Windows) until it is (we only force the unmapping on SUN VMs).
    // Additionally, we need to make sure to delete the data file first, so on restart the others
    // will be recognized as GCable.
    private static final Queue<Runnable> failedDeletions = new ConcurrentLinkedQueue<>();

    TransactionLog(OperationType opType, CFMetaData metadata)
    {
        this(opType, metadata, null);
    }

    TransactionLog(OperationType opType, CFMetaData metadata, Tracker tracker)
    {
        this(opType, new Directories(metadata), tracker);
    }

    TransactionLog(OperationType opType, Directories directories, Tracker tracker)
    {
        this(opType, directories.getDirectoryForNewSSTables(), tracker);
    }

    TransactionLog(OperationType opType, File folder, Tracker tracker)
    {
        this.tracker = tracker;
        this.data = new TransactionData(opType,
                                        folder,
                                        UUIDGen.getTimeUUID());
        this.selfRef = new Ref<>(this, new TransactionTidier(data));

        if (logger.isDebugEnabled())
            logger.debug("Created transaction logs with id {}", data.id);
    }

    /**
     * Track a reader as new.
     **/
    void trackNew(SSTable table)
    {
        if (!data.file.add(RecordType.ADD, table))
            throw new IllegalStateException(table + " is already tracked as new");
    }

    /**
     * Stop tracking a reader as new.
     */
    void untrackNew(SSTable table)
    {
        data.file.remove(RecordType.ADD, table);
    }

    /**
     * Schedule a reader for deletion as soon as it is fully unreferenced and the transaction
     * has been committed.
     */
    SSTableTidier obsoleted(SSTableReader reader)
    {
        if (data.file.contains(RecordType.ADD, reader))
        {
            if (data.file.contains(RecordType.REMOVE, reader))
                throw new IllegalArgumentException();

            return new SSTableTidier(reader, true, this);
        }

        if (!data.file.add(RecordType.REMOVE, reader))
            throw new IllegalStateException();

        if (tracker != null)
            tracker.notifyDeleting(reader);

        return new SSTableTidier(reader, false, this);
    }

    OperationType getType()
    {
        return data.getType();
    }

    UUID getId()
    {
        return data.getId();
    }

    @VisibleForTesting
    String getDataFolder()
    {
        return data.getFolder();
    }

    @VisibleForTesting
    TransactionData getData()
    {
        return data;
    }

    private static void delete(File file)
    {
        try
        {
            if (logger.isDebugEnabled())
                logger.debug("Deleting {}", file);

            Files.delete(file.toPath());
        }
        catch (NoSuchFileException e)
        {
            logger.error("Unable to delete {} as it does not exist", file);
        }
        catch (IOException e)
        {
            logger.error("Unable to delete {}", file, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * The transaction tidier.
     *
     * When the transaction reference is fully released we try to delete all the obsolete files
     * depending on the transaction result, as well as the transaction log file.
     */
    private static class TransactionTidier implements RefCounted.Tidy, Runnable
    {
        private final TransactionData data;

        public TransactionTidier(TransactionData data)
        {
            this.data = data;
        }

        public void tidy() throws Exception
        {
            run();
        }

        public String name()
        {
            return data.toString();
        }

        public void run()
        {
            if (logger.isDebugEnabled())
                logger.debug("Removing files for transaction {}", name());

            assert data.completed() : "Expected a completed transaction: " + data;

            Throwable err = data.removeUnfinishedLeftovers(null);

            if (err != null)
            {
                logger.info("Failed deleting files for transaction {}, we'll retry after GC and on on server restart", name(), err);
                failedDeletions.add(this);
            }
            else
            {
                if (logger.isDebugEnabled())
                    logger.debug("Closing file transaction {}", name());
                data.close();
            }
        }
    }

    static class Obsoletion
    {
        final SSTableReader reader;
        final SSTableTidier tidier;

        public Obsoletion(SSTableReader reader, SSTableTidier tidier)
        {
            this.reader = reader;
            this.tidier = tidier;
        }
    }

    /**
     * The SSTableReader tidier. When a reader is fully released and no longer referenced
     * by any one, we run this. It keeps a reference to the parent transaction and releases
     * it when done, so that the final transaction cleanup can run when all obsolete readers
     * are released.
     */
    public static class SSTableTidier implements Runnable
    {
        // must not retain a reference to the SSTableReader, else leak detection cannot kick in
        private final Descriptor desc;
        private final long sizeOnDisk;
        private final Tracker tracker;
        private final boolean wasNew;
        private final Ref<TransactionLog> parentRef;

        public SSTableTidier(SSTableReader referent, boolean wasNew, TransactionLog parent)
        {
            this.desc = referent.descriptor;
            this.sizeOnDisk = referent.bytesOnDisk();
            this.tracker = parent.tracker;
            this.wasNew = wasNew;
            this.parentRef = parent.selfRef.tryRef();
        }

        public void run()
        {
            SystemKeyspace.clearSSTableReadMeter(desc.ksname, desc.cfname, desc.generation);

            try
            {
                // If we can't successfully delete the DATA component, set the task to be retried later: see TransactionTidier
                File datafile = new File(desc.filenameFor(Component.DATA));

                delete(datafile);
                // let the remainder be cleaned up by delete
                SSTable.delete(desc, SSTable.discoverComponentsFor(desc));
            }
            catch (Throwable t)
            {
                logger.error("Failed deletion for {}, we'll retry after GC and on server restart", desc);
                failedDeletions.add(this);
                return;
            }

            if (tracker != null && tracker.cfstore != null && !wasNew)
                tracker.cfstore.metric.totalDiskSpaceUsed.dec(sizeOnDisk);

            // release the referent to the parent so that the all transaction files can be released
            parentRef.release();
        }

        public void abort()
        {
            parentRef.release();
        }
    }

    /**
     * Retry all deletions that failed the first time around (presumably b/c the sstable was still mmap'd.)
     * Useful because there are times when we know GC has been invoked; also exposed as an mbean.
     */
    public static void rescheduleFailedDeletions()
    {
        Runnable task;
        while ( null != (task = failedDeletions.poll()))
            ScheduledExecutors.nonPeriodicTasks.submit(task);

        // On Windows, snapshots cannot be deleted so long as a segment of the root element is memory-mapped in NTFS.
        SnapshotDeletingTask.rescheduleFailedTasks();
    }

    /**
     * Deletions run on the nonPeriodicTasks executor, (both failedDeletions or global tidiers in SSTableReader)
     * so by scheduling a new empty task and waiting for it we ensure any prior deletion has completed.
     */
    public static void waitForDeletions()
    {
        FBUtilities.waitOnFuture(ScheduledExecutors.nonPeriodicTasks.schedule(Runnables.doNothing(), 0, TimeUnit.MILLISECONDS));
    }

    @VisibleForTesting
    Throwable complete(Throwable accumulate)
    {
        try
        {
            accumulate = selfRef.ensureReleased(accumulate);
            return accumulate;
        }
        catch (Throwable t)
        {
            logger.error("Failed to complete file transaction {}", getId(), t);
            return Throwables.merge(accumulate, t);
        }
    }

    protected Throwable doCommit(Throwable accumulate)
    {
        data.file.commit();
        return complete(accumulate);
    }

    protected Throwable doAbort(Throwable accumulate)
    {
        data.file.abort();
        return complete(accumulate);
    }

    protected void doPrepare() { }

    /**
     * Called on startup to scan existing folders for any unfinished leftovers of
     * operations that were ongoing when the process exited. Also called by the standalone
     * sstableutil tool when the cleanup option is specified, @see StandaloneSSTableUtil.
     *
     */
    static void removeUnfinishedLeftovers(CFMetaData metadata)
    {
        Throwable accumulate = null;

        for (File dir : new Directories(metadata).getCFDirectories())
        {
            File[] logs = dir.listFiles((dir1, name) -> TransactionData.isLogFile(name));

            for (File log : logs)
            {
                try (TransactionData data = TransactionData.make(log))
                {
                    accumulate = data.readLogFile(accumulate);
                    if (accumulate == null)
                        accumulate = data.removeUnfinishedLeftovers(accumulate);
                    else
                        logger.error("Unexpected disk state: failed to read transaction log {}", log, accumulate);
                }
            }
        }

        if (accumulate != null)
            logger.error("Failed to remove unfinished transaction leftovers", accumulate);
    }

    /**
     * A class for listing files in a folder. If we fail we try a few more times
     * in case we are reading txn log files that are still being mutated.
     */
    static final class FileLister
    {
        // The maximum number of attempts for scanning the folder
        private static final int MAX_ATTEMPTS = 10;

        // The delay between each attempt
        private static final int REATTEMPT_DELAY_MILLIS = 5;

        // The folder to scan
        private final Path folder;

        // The filter determines which files the client wants returned, we pass to the filter
        // the file and its type
        private final BiFunction<File, FileType, Boolean> filter;

        // This determines the behavior when we fail to read a txn log file after a few times (MAX_ATTEMPTS)
        private final OnTxnErr onTxnErr;

        // Each time we scan the folder we increment this counter, we scan at most for MAX_ATTEMPTS
        private int attempts;

        public FileLister(Path folder, BiFunction<File, FileType, Boolean> filter, OnTxnErr onTxnErr)
        {
            this.folder = folder;
            this.filter = filter;
            this.onTxnErr = onTxnErr;
            this.attempts = 0;
        }

        public List<File> list()
        {
            while(true)
            {
                try
                {
                    return attemptList();
                }
                catch (Throwable t)
                {
                    if (attempts >= MAX_ATTEMPTS)
                        throw new RuntimeException(String.format("Failed to list files in %s after multiple attempts, giving up", folder), t);

                    logger.warn("Failed to list files in {} : {}", folder, t.getMessage());
                    try
                    {
                        Thread.sleep(REATTEMPT_DELAY_MILLIS);
                    }
                    catch (InterruptedException e)
                    {
                        logger.error("Interrupted whilst waiting to reattempt listing files in {}, giving up", folder, e);
                        throw new RuntimeException(String.format("Failed to list files in %s due to interruption, giving up", folder), t);
                    }
                }
            }
        }

        List<File> attemptList() throws IOException
        {
            attempts++;

            Map<File, FileType> files = new HashMap<>();
            try (DirectoryStream<Path> in = Files.newDirectoryStream(folder))
            {
                if (!(in instanceof SecureDirectoryStream))
                    noSpamLogger.warn("This platform does not support atomic directory streams (SecureDirectoryStream); " +
                                       "race conditions when loading sstable files could occurr");

                in.forEach(path ->
                           {
                               File file = path.toFile();
                               if (file.isDirectory())
                                   return;

                               if (TransactionData.isLogFile(file.getName()))
                               {
                                   Set<File> tmpFiles = getTemporaryFiles(file);
                                   if (tmpFiles != null)
                                   { // process the txn log file only if we can read it (tmpFiles != null)
                                       tmpFiles.stream().forEach((f) -> files.put(f, FileType.TEMPORARY));
                                       files.put(file, FileType.TXN_LOG);
                                   }
                               }
                               else
                               {
                                   files.putIfAbsent(file, FileType.FINAL);
                               }
                           });
            }

            return files.entrySet().stream()
                        .filter((e) -> filter.apply(e.getKey(), e.getValue()))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        }

        Set<File> getTemporaryFiles(File file)
        {
            try (TransactionData txn = TransactionData.make(file))
            {
                maybeFail(txn.readLogFile(null));
                return txn.getTemporaryFiles();
            }
            catch(Throwable t)
            {
                // We always fail if the onTxnErr is set to THROW or if we haven't
                // reached the maximum number of attempts yet. Otherwise
                // we just log an error and continue as if the txn log file does not exist
                // clients can choose which behavior they want via onTxnLogError
                if (attempts < MAX_ATTEMPTS ||
                    onTxnErr == OnTxnErr.THROW)
                    throw new RuntimeException(t);

                logger.error("Failed to read temporary files of txn log {}", file, t);
                return null; // txn.getTemporaryFiles() could be empty so we must use null to differentiate
            }
        }
    }

    @VisibleForTesting
    static Set<File> getTemporaryFiles(CFMetaData metadata, File folder)
    {
        Set<File> ret = new HashSet<>();

        List<File> directories = new Directories(metadata).getCFDirectories();
        directories.add(folder);
        for (File dir : directories)
            ret.addAll(new FileLister(dir.toPath(),
                                      (file, type) -> type != FileType.FINAL,
                                      OnTxnErr.IGNORE).list());

        return ret;
    }
}
