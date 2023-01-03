/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.lifecycle;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LogRecord.Type;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * A transaction log file. We store transaction records into a log file, which is
 * copied into multiple identical replicas on different disks, @see LogFileReplica.
 *
 * This class supports the transactional logic of LogTransaction and the removing
 * of unfinished leftovers when a transaction is completed, or aborted, or when
 * we clean up on start-up.
 *
 * @see LogTransaction
 */
final class LogFile implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(LogFile.class);

    static String EXT = ".log";
    static char SEP = '_';
    // Log file name format:
    // legacy for BIG format: cc_txn_opname_id.log (where cc is one of the sstable versions defined in BigVersion)
    // other formats: fmt-cc_txn_opname_id.log (where fmt is the format and name and cc is one of its versions)
    static Pattern FILE_REGEX = Pattern.compile(String.format("^((?:[a-z]+-)?.{2}_)?txn_(.*)_(.*)%s$", EXT));

    // A set of physical files on disk, each file is an identical replica
    private final LogReplicaSet replicas = new LogReplicaSet();

    // The transaction records, this set must be ORDER PRESERVING
    private final Set<LogRecord> records = Collections.synchronizedSet(new LinkedHashSet<>()); // TODO: Hack until we fix CASSANDRA-14554
    private final Set<LogRecord> onDiskRecords = Collections.synchronizedSet(new LinkedHashSet<>());

    // The type of the transaction
    private final OperationType type;

    // The unique id of the transaction
    private final TimeUUID id;

    private final Version version = DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion();

    static LogFile make(File logReplica)
    {
        return make(logReplica.name(), Collections.singletonList(logReplica));
    }

    static LogFile make(String fileName, List<File> logReplicas)
    {
        Matcher matcher = LogFile.FILE_REGEX.matcher(fileName);
        boolean matched = matcher.matches();
        assert matched && matcher.groupCount() == 3;

        // For now we don't need this but it is there in case we need to change
        // file format later on, the version is the sstable version as defined in BigFormat
        //String version = matcher.group(1);

        OperationType operationType = OperationType.fromFileName(matcher.group(2));
        TimeUUID id = TimeUUID.fromString(matcher.group(3));

        return new LogFile(operationType, id, logReplicas);
    }

    Throwable syncDirectory(Throwable accumulate)
    {
        return replicas.syncDirectory(accumulate);
    }

    OperationType type()
    {
        return type;
    }

    TimeUUID id()
    {
        return id;
    }

    Throwable removeUnfinishedLeftovers(Throwable accumulate)
    {
        try
        {
            // we sync the parent directories before content deletion to ensure
            // any previously deleted files (see SSTableTider) are not
            // incorrectly picked up by record.getExistingFiles() in
            // deleteRecordFiles(), see CASSANDRA-12261
            Throwables.maybeFail(syncDirectory(accumulate));

            deleteFilesForRecordsOfType(committed() ? Type.REMOVE : Type.ADD);

            // we sync the parent directories between contents and log deletion
            // to ensure there is a happens before edge between them
            Throwables.maybeFail(syncDirectory(accumulate));

            accumulate = replicas.delete(accumulate);
        }
        catch (Throwable t)
        {
            accumulate = merge(accumulate, t);
        }

        return accumulate;
    }

    static boolean isLogFile(File file)
    {
        return LogFile.FILE_REGEX.matcher(file.name()).matches();
    }

    LogFile(OperationType type, TimeUUID id, List<File> replicas)
    {
        this(type, id);
        this.replicas.addReplicas(replicas);
    }

    LogFile(OperationType type, TimeUUID id)
    {
        this.type = type;
        this.id = id;
    }

    boolean verify()
    {
        records.clear();
        if (!replicas.readRecords(records))
        {
            logger.error("Failed to read records for transaction log {}", this);
            return false;
        }

        Set<String> absolutePaths = new HashSet<>();
        for (LogRecord record : records)
            record.absolutePath.ifPresent(absolutePaths::add);

        Map<String, List<File>> recordFiles = LogRecord.getExistingFiles(absolutePaths);
        for (LogRecord record : records)
        {
            List<File> existingFiles = Collections.emptyList();
            if (record.absolutePath.isPresent())
            {
                String key = record.absolutePath.get();
                existingFiles = recordFiles.getOrDefault(key, Collections.emptyList());
            }
            LogFile.verifyRecord(record, existingFiles);
        }

        Optional<LogRecord> firstInvalid = records.stream().filter(LogRecord::isInvalidOrPartial).findFirst();
        if (!firstInvalid.isPresent())
            return true;

        LogRecord failedOn = firstInvalid.get();
        if (getLastRecord() != failedOn)
        {
            setErrorInReplicas(failedOn);
            return false;
        }

        records.stream().filter((r) -> r != failedOn).forEach(LogFile::verifyRecordWithCorruptedLastRecord);
        if (records.stream()
                   .filter((r) -> r != failedOn)
                   .filter(LogRecord::isInvalid)
                   .map(this::setErrorInReplicas)
                   .findFirst().isPresent())
        {
            setErrorInReplicas(failedOn);
            return false;
        }

        // if only the last record is corrupt and all other records have matching files on disk, @see verifyRecord,
        // then we simply exited whilst serializing the last record and we carry on
        logger.warn("Last record of transaction {} is corrupt or incomplete [{}], " +
                    "but all previous records match state on disk; continuing",
                    id, failedOn.error());
        return true;
    }

    LogRecord setErrorInReplicas(LogRecord record)
    {
        replicas.setErrorInReplicas(record);
        return record;
    }

    static void verifyRecord(LogRecord record, List<File> existingFiles)
    {
        if (record.checksum != record.computeChecksum())
        {
            record.setError(String.format("Invalid checksum for sstable [%s]: [%d] should have been [%d]",
                                          record.fileName(),
                                          record.checksum,
                                          record.computeChecksum()));
            return;
        }

        if (record.type != Type.REMOVE)
            return;

        // Paranoid sanity checks: we create another record by looking at the files as they are
        // on disk right now and make sure the information still matches. We don't want to delete
        // files by mistake if the user has copied them from backup and forgot to remove a txn log
        // file that obsoleted the very same files. So we check the latest update time and make sure
        // it matches. Because we delete files from oldest to newest, the latest update time should
        // always match.
        record.status.onDiskRecord = record.withExistingFiles(existingFiles);
        // we can have transaction files with mismatching updateTime resolutions due to switching between jdk8 and jdk11, truncate both to be consistent:
        if (truncateMillis(record.updateTime) != truncateMillis(record.status.onDiskRecord.updateTime) && record.status.onDiskRecord.updateTime > 0)
        {
            record.setError(String.format("Unexpected files detected for sstable [%s]: " +
                                          "last update time [%tc] (%d) should have been [%tc] (%d)",
                                          record.fileName(),
                                          record.status.onDiskRecord.updateTime,
                                          record.status.onDiskRecord.updateTime,
                                          record.updateTime,
                                          record.updateTime));

        }
    }

    /**
     * due to difference in timestamp resolution between jdk8 and 11 we need to return second resolution here (number
     * should end in 000): https://bugs.openjdk.java.net/browse/JDK-8177809
     */
    static long truncateMillis(long lastModified)
    {
        return lastModified - (lastModified % 1000);
    }


    static void verifyRecordWithCorruptedLastRecord(LogRecord record)
    {
        if (record.type == Type.REMOVE && record.status.onDiskRecord.numFiles < record.numFiles)
        { // if we found a corruption in the last record, then we continue only
          // if the number of files matches exactly for all previous records.
            record.setError(String.format("Incomplete fileset detected for sstable [%s]: " +
                                          "number of files [%d] should have been [%d].",
                                          record.fileName(),
                                          record.status.onDiskRecord.numFiles,
                                          record.numFiles));
        }
    }

    void commit()
    {
        addRecord(LogRecord.makeCommit(currentTimeMillis()));
    }

    void abort()
    {
        addRecord(LogRecord.makeAbort(currentTimeMillis()));
    }

    private boolean isLastRecordValidWithType(Type type)
    {
        LogRecord lastRecord = getLastRecord();
        return lastRecord != null &&
               lastRecord.type == type &&
               lastRecord.isValid();
    }

    boolean committed()
    {
        return isLastRecordValidWithType(Type.COMMIT);
    }

    boolean aborted()
    {
        return isLastRecordValidWithType(Type.ABORT);
    }

    boolean completed()
    {
        return committed() || aborted();
    }

    void add(SSTable table)
    {
        addRecord(makeAddRecord(table));
    }

    public void addAll(Type type, Iterable<SSTableReader> toBulkAdd)
    {
        for (LogRecord record : makeRecords(type, toBulkAdd).values())
            addRecord(record);
    }

    Map<SSTable, LogRecord> makeRecords(Type type, Iterable<SSTableReader> tables)
    {
        assert type == Type.ADD || type == Type.REMOVE;

        for (SSTableReader sstable : tables)
            maybeCreateReplica(sstable);
        return LogRecord.make(type, tables);
    }

    private LogRecord makeAddRecord(SSTable table)
    {
        maybeCreateReplica(table);
        return LogRecord.make(Type.ADD, table);
    }

    /**
     * this version of makeRecord takes an existing LogRecord and converts it to a
     * record with the given type. This avoids listing the directory and if the
     * LogRecord already exists, we have all components for the sstable
     */
    private LogRecord makeRecord(Type type, SSTable table, LogRecord record)
    {
        assert type == Type.ADD || type == Type.REMOVE;
        maybeCreateReplica(table);
        return record.asType(type);
    }

    private void maybeCreateReplica(SSTable sstable)
    {
        File directory = sstable.descriptor.directory;
        String fileName = StringUtils.join(directory, File.pathSeparator(), getFileName());
        replicas.maybeCreateReplica(directory, fileName, onDiskRecords);
    }

    void addRecord(LogRecord record)
    {
        if (completed())
            throw new IllegalStateException("Transaction already completed");

        if (records.contains(record))
            throw new IllegalStateException("Record already exists");

        replicas.append(record);
        if (!records.add(record))
            throw new IllegalStateException("Failed to add record");
        onDiskRecords.add(record);
    }

    void remove(SSTable table)
    {
        LogRecord record = makeAddRecord(table);
        assert records.contains(record) : String.format("[%s] is not tracked by %s", record, id);
        assert record.absolutePath.isPresent();
        deleteRecordFiles(LogRecord.getExistingFiles(record.absolutePath.get()));
        records.remove(record);
    }

    boolean contains(Type type, SSTable sstable, LogRecord record)
    {
        return contains(makeRecord(type, sstable, record));
    }

    private boolean contains(LogRecord record)
    {
        return records.contains(record);
    }

    void deleteFilesForRecordsOfType(Type type)
    {
        assert type == Type.REMOVE || type == Type.ADD;
        Set<String> absolutePaths = new HashSet<>();
        for (LogRecord record : records)
        {
            if (type.matches(record))
            {
                assert record.absolutePath.isPresent() : "type is either REMOVE or ADD, record should always have an absolutePath: " + record;
                absolutePaths.add(record.absolutePath.get());
            }
        }

        Map<String, List<File>> existingFiles = LogRecord.getExistingFiles(absolutePaths);

        for (List<File> toDelete : existingFiles.values())
            LogFile.deleteRecordFiles(toDelete);

        records.clear();
    }

    private static void deleteRecordFiles(List<File> existingFiles)
    {
        // we sort the files in ascending update time order so that the last update time
        // stays the same even if we only partially delete files, see comment in isInvalid()
        existingFiles.sort(Comparator.comparingLong(File::lastModified));
        existingFiles.forEach(LogTransaction::delete);
    }

    /**
     * Extract from the files passed in all those that are of the given type.
     *
     * Scan all records and select those that are of the given type, valid, and
     * located in the same folder. For each such record extract from the files passed in
     * those that belong to this record.
     *
     * @return a map linking each mapped record to its files, where the files where passed in as parameters.
     */
    Map<LogRecord, Set<File>> getFilesOfType(Path folder, NavigableSet<File> files, Type type)
    {
        Map<LogRecord, Set<File>> ret = new HashMap<>();

        records.stream()
               .filter(type::matches)
               .filter(LogRecord::isValid)
               .filter(r -> r.isInFolder(folder))
               .forEach((r) -> ret.put(r, getRecordFiles(files, r)));

        return ret;
    }

    LogRecord getLastRecord()
    {
        return Iterables.getLast(records, null);
    }

    private static Set<File> getRecordFiles(NavigableSet<File> files, LogRecord record)
    {
        String fileName = record.fileName();
        return files.stream().filter(f -> f.name().startsWith(fileName)).collect(Collectors.toSet());
    }

    boolean exists()
    {
        return replicas.exists();
    }

    public void close()
    {
        replicas.close();
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean showContents)
    {
        StringBuilder str = new StringBuilder();
        str.append('[');
        str.append(getFileName());
        str.append(" in ");
        str.append(replicas.getDirectories());
        str.append(']');
        if (showContents)
        {
            str.append(System.lineSeparator());
            str.append("Files and contents follow:");
            str.append(System.lineSeparator());
            replicas.printContentsWithAnyErrors(str);
        }
        return str.toString();
    }

    @VisibleForTesting
    List<File> getFiles()
    {
        return replicas.getFiles();
    }

    @VisibleForTesting
    List<String> getFilePaths()
    {
        return replicas.getFilePaths();
    }

    private String getFileName()
    {
        // For pre-5.0 versions, only BigFormat is supported, and the file name includes only the version string.
        // To retain the ability to downgrade to 4.x, we keep the old file naming scheme for BigFormat sstables
        // and add format names for other formats as they are supported only in 5.0 and above.
        return StringUtils.join(BigFormat.is(version.format) ? version.toString() : version.toFormatAndVersionString(), LogFile.SEP, // remove version and separator when downgrading to 4.x is becomes unsupported
                                "txn", LogFile.SEP,
                                type.fileName, LogFile.SEP,
                                id.toString(), LogFile.EXT);
    }

    public boolean isEmpty()
    {
        return records.isEmpty();
    }
}