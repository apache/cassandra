package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LogRecord.Type;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.utils.Throwables;

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
final class LogFile
{
    private static final Logger logger = LoggerFactory.getLogger(LogFile.class);

    static String EXT = ".log";
    static char SEP = '_';
    // cc_txn_opname_id.log (where cc is one of the sstable versions defined in BigVersion)
    static Pattern FILE_REGEX = Pattern.compile(String.format("^(.{2})_txn_(.*)_(.*)%s$", EXT));

    // A set of physical files on disk, each file is an identical replica
    private final LogReplicaSet replicas = new LogReplicaSet();

    // The transaction records, this set must be ORDER PRESERVING
    private final LinkedHashSet<LogRecord> records = new LinkedHashSet<>();

    // The type of the transaction
    private final OperationType type;

    // The unique id of the transaction
    private final UUID id;

    static LogFile make(File logReplica)
    {
        return make(logReplica.getName(), Collections.singletonList(logReplica));
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
        UUID id = UUID.fromString(matcher.group(3));

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

    UUID id()
    {
        return id;
    }

    Throwable removeUnfinishedLeftovers(Throwable accumulate)
    {
        try
        {
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
        return LogFile.FILE_REGEX.matcher(file.getName()).matches();
    }

    LogFile(OperationType type, UUID id, List<File> replicas)
    {
        this(type, id);
        this.replicas.addReplicas(replicas);
    }

    LogFile(OperationType type, UUID id)
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

        records.forEach(LogFile::verifyRecord);

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
        logger.warn(String.format("Last record of transaction %s is corrupt or incomplete [%s], " +
                                  "but all previous records match state on disk; continuing",
                                  id,
                                  failedOn.error()));
        return true;
    }

    LogRecord setErrorInReplicas(LogRecord record)
    {
        replicas.setErrorInReplicas(record);
        return record;
    }

    static void verifyRecord(LogRecord record)
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
        record.status.onDiskRecord = record.withExistingFiles();
        if (record.updateTime != record.status.onDiskRecord.updateTime && record.status.onDiskRecord.numFiles > 0)
        {
            record.setError(String.format("Unexpected files detected for sstable [%s]: " +
                                          "last update time [%tT] should have been [%tT]",
                                          record.fileName(),
                                          record.status.onDiskRecord.updateTime,
                                          record.updateTime));

        }
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
        assert !completed() : "Already completed!";
        addRecord(LogRecord.makeCommit(System.currentTimeMillis()));
    }

    void abort()
    {
        assert !completed() : "Already completed!";
        addRecord(LogRecord.makeAbort(System.currentTimeMillis()));
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

    void add(Type type, SSTable table)
    {
        if (!addRecord(makeRecord(type, table)))
            throw new IllegalStateException();
    }

    private LogRecord makeRecord(Type type, SSTable table)
    {
        assert type == Type.ADD || type == Type.REMOVE;

        File directory = table.descriptor.directory;
        String fileName = StringUtils.join(directory, File.separator, getFileName());
        replicas.maybeCreateReplica(directory, fileName, records);
        return LogRecord.make(type, table);
    }

    private boolean addRecord(LogRecord record)
    {
        if (records.contains(record))
            return false;

        replicas.append(record);

        return records.add(record);
    }

    void remove(Type type, SSTable table)
    {
        LogRecord record = makeRecord(type, table);
        assert records.contains(record) : String.format("[%s] is not tracked by %s", record, id);

        deleteRecordFiles(record);
        records.remove(record);
    }

    boolean contains(Type type, SSTable table)
    {
        return records.contains(makeRecord(type, table));
    }

    void deleteFilesForRecordsOfType(Type type)
    {
        records.stream()
               .filter(type::matches)
               .forEach(LogFile::deleteRecordFiles);
        records.clear();
    }

    private static void deleteRecordFiles(LogRecord record)
    {
        List<File> files = record.getExistingFiles();

        // we sort the files in ascending update time order so that the last update time
        // stays the same even if we only partially delete files, see comment in isInvalid()
        files.sort((f1, f2) -> Long.compare(f1.lastModified(), f2.lastModified()));

        files.forEach(LogTransaction::delete);
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
        return files.stream().filter(f -> f.getName().startsWith(fileName)).collect(Collectors.toSet());
    }

    boolean exists()
    {
        return replicas.exists();
    }

    void close()
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
        return StringUtils.join(BigFormat.latestVersion,
                                LogFile.SEP,
                                "txn",
                                LogFile.SEP,
                                type.fileName,
                                LogFile.SEP,
                                id.toString(),
                                LogFile.EXT);
    }
}
