package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Iterables;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LogRecord.Type;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CLibrary;

import static org.apache.cassandra.utils.Throwables.merge;

/**
 * The transaction log file, which contains many records.
 */
final class LogFile
{
    private static final Logger logger = LoggerFactory.getLogger(LogFile.class);

    static String EXT = ".log";
    static char SEP = '_';
    // cc_txn_opname_id.log (where cc is one of the sstable versions defined in BigVersion)
    static Pattern FILE_REGEX = Pattern.compile(String.format("^(.{2})_txn_(.*)_(.*)%s$", EXT));

    final File file;
    final Set<LogRecord> records = new LinkedHashSet<>();
    final OperationType opType;
    final UUID id;
    final File folder;
    final int folderDescriptor;

    static LogFile make(File logFile, int folderDescriptor)
    {
        Matcher matcher = LogFile.FILE_REGEX.matcher(logFile.getName());
        assert matcher.matches() && matcher.groupCount() == 3;

        // For now we don't need this but it is there in case we need to change
        // file format later on, the version is the sstable version as defined in BigFormat
        //String version = matcher.group(1);

        OperationType operationType = OperationType.fromFileName(matcher.group(2));
        UUID id = UUID.fromString(matcher.group(3));

        return new LogFile(operationType, logFile.getParentFile(), folderDescriptor, id);
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

    Throwable removeUnfinishedLeftovers(Throwable accumulate)
    {
        try
        {
            deleteRecords(committed() ? Type.REMOVE : Type.ADD);

            // we sync the parent file descriptor between contents and log deletion
            // to ensure there is a happens before edge between them
            sync();

            Files.delete(file.toPath());
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

    LogFile(OperationType opType, File folder, int folderDescriptor, UUID id)
    {
        this.opType = opType;
        this.id = id;
        this.folder = folder;
        this.file = new File(getFileName(folder, opType, id));
        this.folderDescriptor = folderDescriptor;
    }

    public void readRecords()
    {
        assert records.isEmpty();
        FileUtils.readLines(file).stream()
                 .map(LogRecord::make)
                 .forEach(records::add);
    }

    public boolean verify()
    {
        Optional<LogRecord> firstInvalid = records.stream()
                                                  .filter(this::isInvalid)
                                                  .findFirst();

        if (!firstInvalid.isPresent())
            return true;

        LogRecord failedOn = firstInvalid.get();
        if (getLastRecord() != failedOn)
        {
            logError(failedOn);
            return false;
        }

        if (records.stream()
                   .filter((r) -> r != failedOn)
                   .filter(LogFile::isInvalidWithCorruptedLastRecord)
                   .map(LogFile::logError)
                   .findFirst().isPresent())
        {
            logError(failedOn);
            return false;
        }

        // if only the last record is corrupt and all other records have matching files on disk, @see verifyRecord,
        // then we simply exited whilst serializing the last record and we carry on
        logger.warn(String.format("Last record of transaction %s is corrupt or incomplete [%s], but all previous records match state on disk; continuing",
                                  id,
                                  failedOn.error));
        return true;
    }

    static LogRecord logError(LogRecord record)
    {
        logger.error("{}", record.error);
        return record;
    }

    boolean isInvalid(LogRecord record)
    {
        if (!record.isValid())
            return true;

        if (record.type == Type.UNKNOWN)
        {
            record.error(String.format("Could not parse record [%s]", record));
            return true;
        }

        if (record.checksum != record.computeChecksum())
        {
            record.error(String.format("Invalid checksum for sstable [%s], record [%s]: [%d] should have been [%d]",
                                       record.relativeFilePath,
                                       record,
                                       record.checksum,
                                       record.computeChecksum()));
            return true;
        }

        if (record.type != Type.REMOVE)
            return false;

        List<File> files = record.getExistingFiles(folder);

        // Paranoid sanity checks: we create another record by looking at the files as they are
        // on disk right now and make sure the information still matches
        record.onDiskRecord = LogRecord.make(record.type, files, 0, record.relativeFilePath);

        if (record.updateTime != record.onDiskRecord.updateTime && record.onDiskRecord.numFiles > 0)
        {
            record.error(String.format("Unexpected files detected for sstable [%s], record [%s]: last update time [%tT] should have been [%tT]",
                                       record.relativeFilePath,
                                       record,
                                       record.onDiskRecord.updateTime,
                                       record.updateTime));
            return true;
        }

        return false;
    }

    static boolean isInvalidWithCorruptedLastRecord(LogRecord record)
    {
        if (record.type == Type.REMOVE && record.onDiskRecord.numFiles < record.numFiles)
        { // if we found a corruption in the last record, then we continue only if the number of files matches exactly for all previous records.
            record.error(String.format("Incomplete fileset detected for sstable [%s], record [%s]: number of files [%d] should have been [%d]. Treating as unrecoverable due to corruption of the final record.",
                         record.relativeFilePath,
                         record.raw,
                         record.onDiskRecord.numFiles,
                         record.numFiles));
            return true;
        }
        return false;
    }

    public void commit()
    {
        assert !completed() : "Already completed!";
        addRecord(LogRecord.makeCommit(System.currentTimeMillis()));
    }

    public void abort()
    {
        assert !completed() : "Already completed!";
        addRecord(LogRecord.makeAbort(System.currentTimeMillis()));
    }

    private boolean isLastRecordValidWithType(Type type)
    {
        LogRecord lastRecord = getLastRecord();
        return lastRecord != null &&
               lastRecord.type == type &&
               !isInvalid(lastRecord);
    }

    public boolean committed()
    {
        return isLastRecordValidWithType(Type.COMMIT);
    }

    public boolean aborted()
    {
        return isLastRecordValidWithType(Type.ABORT);
    }

    public boolean completed()
    {
        return committed() || aborted();
    }

    public void add(Type type, SSTable table)
    {
        if (!addRecord(makeRecord(type, table)))
            throw new IllegalStateException();
    }

    private LogRecord makeRecord(Type type, SSTable table)
    {
        assert type == Type.ADD || type == Type.REMOVE;
        return LogRecord.make(type, folder, table);
    }

    private boolean addRecord(LogRecord record)
    {
        if (!records.add(record))
            return false;

        // we only checksum the records, not the checksums themselves
        FileUtils.append(file, record.toString());
        sync();
        return true;
    }

    public void remove(Type type, SSTable table)
    {
        LogRecord record = makeRecord(type, table);

        assert records.contains(record) : String.format("[%s] is not tracked by %s", record, file);

        records.remove(record);
        deleteRecord(record);
    }

    public boolean contains(Type type, SSTable table)
    {
        return records.contains(makeRecord(type, table));
    }

    public void deleteRecords(Type type)
    {
        assert file.exists() : String.format("Expected %s to exists", file);
        records.stream()
               .filter(type::matches)
               .forEach(this::deleteRecord);
        records.clear();
    }

    private void deleteRecord(LogRecord record)
    {
        List<File> files = record.getExistingFiles(folder);

        // we sort the files in ascending update time order so that the last update time
        // stays the same even if we only partially delete files
        files.sort((f1, f2) -> Long.compare(f1.lastModified(), f2.lastModified()));

        files.forEach(LogTransaction::delete);
    }

    public Map<LogRecord, Set<File>> getFilesOfType(NavigableSet<File> files, Type type)
    {
        Map<LogRecord, Set<File>> ret = new HashMap<>();

        records.stream()
               .filter(type::matches)
               .filter(LogRecord::isValid)
               .forEach((r) -> ret.put(r, getRecordFiles(files, r)));

        return ret;
    }

    public LogRecord getLastRecord()
    {
        return Iterables.getLast(records, null);
    }

    private Set<File> getRecordFiles(NavigableSet<File> files, LogRecord record)
    {
        Set<File> ret = new HashSet<>();
        for (File file : files.tailSet(new File(folder, record.relativeFilePath)))
        {
            if (!file.getName().startsWith(record.relativeFilePath))
                break;
            ret.add(file);
        }
        return ret;
    }

    public void delete()
    {
        LogTransaction.delete(file);
    }

    public boolean exists()
    {
        return file.exists();
    }

    @Override
    public String toString()
    {
        return FileUtils.getRelativePath(folder.getPath(), file.getPath());
    }

    static String getFileName(File folder, OperationType opType, UUID id)
    {
        String fileName = StringUtils.join(BigFormat.latestVersion,
                                           LogFile.SEP,
                                           "txn",
                                           LogFile.SEP,
                                           opType.fileName,
                                           LogFile.SEP,
                                           id.toString(),
                                           LogFile.EXT);
        return StringUtils.join(folder, File.separator, fileName);
    }
}

