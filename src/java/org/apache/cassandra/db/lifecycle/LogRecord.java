package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A log file record, each record is encoded in one line and has different
 * content depending on the record type.
 */
final class LogRecord
{
    public enum Type
    {
        UNKNOWN, // a record that cannot be parsed
        ADD,    // new files to be retained on commit
        REMOVE, // old files to be retained on abort
        COMMIT, // commit flag
        ABORT;  // abort flag

        public static Type fromPrefix(String prefix)
        {
            return valueOf(prefix.toUpperCase());
        }

        public boolean hasFile()
        {
            return this == Type.ADD || this == Type.REMOVE;
        }

        public boolean matches(LogRecord record)
        {
            return this == record.type;
        }
    }


    public final Type type;
    public final String relativeFilePath;
    public final long updateTime;
    public final int numFiles;
    public final String raw;
    public final long checksum;

    public String error;
    public LogRecord onDiskRecord;

    // (add|remove|commit|abort):[*,*,*][checksum]
    static Pattern REGEX = Pattern.compile("^(add|remove|commit|abort):\\[([^,]*),?([^,]*),?([^,]*)\\]\\[(\\d*)\\]$", Pattern.CASE_INSENSITIVE);

    public static LogRecord make(String line)
    {
        try
        {
            Matcher matcher = REGEX.matcher(line);
            if (!matcher.matches())
                return new LogRecord(Type.UNKNOWN, "", 0, 0, 0, line)
                       .error(String.format("Failed to parse [%s]", line));

            Type type = Type.fromPrefix(matcher.group(1));
            return new LogRecord(type, matcher.group(2), Long.valueOf(matcher.group(3)), Integer.valueOf(matcher.group(4)), Long.valueOf(matcher.group(5)), line);
        }
        catch (Throwable t)
        {
            return new LogRecord(Type.UNKNOWN, "", 0, 0, 0, line).error(t);
        }
    }

    public static LogRecord makeCommit(long updateTime)
    {
        return new LogRecord(Type.COMMIT, "", updateTime, 0);
    }

    public static LogRecord makeAbort(long updateTime)
    {
        return new LogRecord(Type.ABORT, "", updateTime, 0);
    }

    public static LogRecord make(Type type, File parentFolder, SSTable table)
    {
        String relativePath = FileUtils.getRelativePath(parentFolder.getPath(), table.descriptor.baseFilename());
        // why do we take the max of files.size() and table.getAllFilePaths().size()?
        return make(type, getExistingFiles(parentFolder, relativePath), table.getAllFilePaths().size(), relativePath);
    }

    public static LogRecord make(Type type, List<File> files, int minFiles, String relativeFilePath)
    {
        long lastModified = files.stream().map(File::lastModified).reduce(0L, Long::max);
        return new LogRecord(type, relativeFilePath, lastModified, Math.max(minFiles, files.size()));
    }

    private LogRecord(Type type,
                      String relativeFilePath,
                      long updateTime,
                      int numFiles)
    {
        this(type, relativeFilePath, updateTime, numFiles, 0, null);
    }

    private LogRecord(Type type,
                      String relativeFilePath,
                      long updateTime,
                      int numFiles,
                      long checksum,
                      String raw)
    {
        this.type = type;
        this.relativeFilePath = type.hasFile() ? relativeFilePath : ""; // only meaningful for file records
        this.updateTime = type == Type.REMOVE ? updateTime : 0; // only meaningful for old records
        this.numFiles = type.hasFile() ? numFiles : 0; // only meaningful for file records
        if (raw == null)
        {
            assert checksum == 0;
            this.checksum = computeChecksum();
            this.raw = format();
        }
        else
        {
            this.checksum = checksum;
            this.raw = raw;
        }

        this.error = "";
    }

    public LogRecord error(Throwable t)
    {
        return error(t.getMessage());
    }

    public LogRecord error(String error)
    {
        this.error = error;
        return this;
    }

    public boolean isValid()
    {
        return this.error.isEmpty();
    }

    private String format()
    {
        return String.format("%s:[%s,%d,%d][%d]", type.toString(), relativeFilePath, updateTime, numFiles, checksum);
    }

    public List<File> getExistingFiles(File folder)
    {
        if (!type.hasFile())
            return Collections.emptyList();

        return getExistingFiles(folder, relativeFilePath);
    }

    public static List<File> getExistingFiles(File parentFolder, String relativeFilePath)
    {
        return Arrays.asList(parentFolder.listFiles((dir, name) -> name.startsWith(relativeFilePath)));
    }

    @Override
    public int hashCode()
    {
        // see comment in equals
        return Objects.hash(type, relativeFilePath, error);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof LogRecord))
            return false;

        final LogRecord other = (LogRecord)obj;

        // we exclude on purpose checksum, update time and count as
        // we don't want duplicated records that differ only by
        // properties that might change on disk, especially COMMIT records,
        // there should be only one regardless of update time
        // however we must compare the error to make sure we have more than
        // one UNKNOWN record, if we fail to parse more than one
        return type == other.type &&
               relativeFilePath.equals(other.relativeFilePath) &&
               error.equals(other.error);
    }

    @Override
    public String toString()
    {
        return raw;
    }

    long computeChecksum()
    {
        CRC32 crc32 = new CRC32();
        crc32.update(relativeFilePath.getBytes(FileUtils.CHARSET));
        crc32.update(type.toString().getBytes(FileUtils.CHARSET));
        FBUtilities.updateChecksumInt(crc32, (int) updateTime);
        FBUtilities.updateChecksumInt(crc32, (int) (updateTime >>> 32));
        FBUtilities.updateChecksumInt(crc32, numFiles);
        return crc32.getValue() & (Long.MAX_VALUE);
    }
}
