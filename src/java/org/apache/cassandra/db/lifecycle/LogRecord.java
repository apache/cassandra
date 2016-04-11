package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A decoded line in a transaction log file replica.
 *
 * @see LogReplica and LogFile.
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

        public boolean isFinal() { return this == Type.COMMIT || this == Type.ABORT; }
    }

    /**
     * The status of a record after it has been verified, any parsing errors
     * are also store here.
     */
    public final static class Status
    {
        // if there are any errors, they end up here
        Optional<String> error = Optional.empty();

        // if the record was only partially matched across files this is true
        boolean partial = false;

        // if the status of this record on disk is required (e.g. existing files), it is
        // stored here for caching
        LogRecord onDiskRecord;

        void setError(String error)
        {
            if (!this.error.isPresent())
                this.error = Optional.of(error);
        }

        boolean hasError()
        {
            return error.isPresent();
        }
    }

    // the type of record, see Type
    public final Type type;
    // for sstable records, the absolute path of the table desc
    public final Optional<String> absolutePath;
    // for sstable records, the last update time of all files (may not be available for NEW records)
    public final long updateTime;
    // for sstable records, the total number of files (may not be accurate for NEW records)
    public final int numFiles;
    // the raw string as written or read from a file
    public final String raw;
    // the checksum of this record, written at the end of the record string
    public final long checksum;
    // the status of this record, @see Status class
    public final Status status;

    // (add|remove|commit|abort):[*,*,*][checksum]
    static Pattern REGEX = Pattern.compile("^(add|remove|commit|abort):\\[([^,]*),?([^,]*),?([^,]*)\\]\\[(\\d*)\\]$", Pattern.CASE_INSENSITIVE);

    public static LogRecord make(String line)
    {
        try
        {
            Matcher matcher = REGEX.matcher(line);
            if (!matcher.matches())
                return new LogRecord(Type.UNKNOWN, null, 0, 0, 0, line)
                       .setError(String.format("Failed to parse [%s]", line));

            Type type = Type.fromPrefix(matcher.group(1));
            return new LogRecord(type,
                                 matcher.group(2),
                                 Long.valueOf(matcher.group(3)),
                                 Integer.valueOf(matcher.group(4)),
                                 Long.valueOf(matcher.group(5)),
                                 line);
        }
        catch (IllegalArgumentException e)
        {
            return new LogRecord(Type.UNKNOWN, null, 0, 0, 0, line)
                   .setError(String.format("Failed to parse line: %s", e.getMessage()));
        }
    }

    public static LogRecord makeCommit(long updateTime)
    {
        return new LogRecord(Type.COMMIT, updateTime);
    }

    public static LogRecord makeAbort(long updateTime)
    {
        return new LogRecord(Type.ABORT, updateTime);
    }

    public static LogRecord make(Type type, SSTable table)
    {
        String absoluteTablePath = FileUtils.getCanonicalPath(table.descriptor.baseFilename());
        return make(type, getExistingFiles(absoluteTablePath), table.getAllFilePaths().size(), absoluteTablePath);
    }

    public LogRecord withExistingFiles()
    {
        return make(type, getExistingFiles(), 0, absolutePath.get());
    }

    public static LogRecord make(Type type, List<File> files, int minFiles, String absolutePath)
    {
        long lastModified = files.stream().map(File::lastModified).reduce(0L, Long::max);
        return new LogRecord(type, absolutePath, lastModified, Math.max(minFiles, files.size()));
    }

    private LogRecord(Type type, long updateTime)
    {
        this(type, null, updateTime, 0, 0, null);
    }

    private LogRecord(Type type,
                      String absolutePath,
                      long updateTime,
                      int numFiles)
    {
        this(type, absolutePath, updateTime, numFiles, 0, null);
    }

    private LogRecord(Type type,
                      String absolutePath,
                      long updateTime,
                      int numFiles,
                      long checksum,
                      String raw)
    {
        assert !type.hasFile() || absolutePath != null : "Expected file path for file records";

        this.type = type;
        this.absolutePath = type.hasFile() ? Optional.of(absolutePath) : Optional.<String>empty();
        this.updateTime = type == Type.REMOVE ? updateTime : 0;
        this.numFiles = type.hasFile() ? numFiles : 0;
        this.status = new Status();
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
    }

    LogRecord setError(String error)
    {
        status.setError(error);
        return this;
    }

    String error()
    {
        return status.error.orElse("");
    }

    void setPartial()
    {
        status.partial = true;
    }

    boolean partial()
    {
        return status.partial;
    }

    boolean isValid()
    {
        return !status.hasError() && type != Type.UNKNOWN;
    }

    boolean isInvalid()
    {
        return !isValid();
    }

    boolean isInvalidOrPartial()
    {
        return isInvalid() || partial();
    }

    private String format()
    {
        return String.format("%s:[%s,%d,%d][%d]",
                             type.toString(),
                             absolutePath(),
                             updateTime,
                             numFiles,
                             checksum);
    }

    public List<File> getExistingFiles()
    {
        assert absolutePath.isPresent() : "Expected a path in order to get existing files";
        return getExistingFiles(absolutePath.get());
    }

    public static List<File> getExistingFiles(String absoluteFilePath)
    {
        Path path = Paths.get(absoluteFilePath);
        File[] files = path.getParent().toFile().listFiles((dir, name) -> name.startsWith(path.getFileName().toString()));
        // files may be null if the directory does not exist yet, e.g. when tracking new files
        return files == null ? Collections.emptyList() : Arrays.asList(files);
    }

    public boolean isFinal()
    {
        return type.isFinal();
    }

    String fileName()
    {
        return absolutePath.isPresent() ? Paths.get(absolutePath.get()).getFileName().toString() : "";
    }

    boolean isInFolder(Path folder)
    {
        return absolutePath.isPresent()
               ? FileUtils.isContained(folder.toFile(), Paths.get(absolutePath.get()).toFile())
               : false;
    }

    String absolutePath()
    {
        return absolutePath.isPresent() ? absolutePath.get() : "";
    }

    @Override
    public int hashCode()
    {
        // see comment in equals
        return Objects.hash(type, absolutePath, numFiles, updateTime);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof LogRecord))
            return false;

        final LogRecord other = (LogRecord)obj;

        // we exclude on purpose checksum, error and full file path
        // since records must match across log file replicas on different disks
        return type == other.type &&
               absolutePath.equals(other.absolutePath) &&
               numFiles == other.numFiles &&
               updateTime == other.updateTime;
    }

    @Override
    public String toString()
    {
        return raw;
    }

    long computeChecksum()
    {
        CRC32 crc32 = new CRC32();
        crc32.update((absolutePath()).getBytes(FileUtils.CHARSET));
        crc32.update(type.toString().getBytes(FileUtils.CHARSET));
        FBUtilities.updateChecksumInt(crc32, (int) updateTime);
        FBUtilities.updateChecksumInt(crc32, (int) (updateTime >>> 32));
        FBUtilities.updateChecksumInt(crc32, numFiles);
        return crc32.getValue() & (Long.MAX_VALUE);
    }
}
