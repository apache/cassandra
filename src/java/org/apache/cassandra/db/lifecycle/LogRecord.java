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

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
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
                                 matcher.group(2) + Component.separator, // see comment on CASSANDRA-13294 below
                                 Long.parseLong(matcher.group(3)),
                                 Integer.parseInt(matcher.group(4)),
                                 Long.parseLong(matcher.group(5)),
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
        // CASSANDRA-13294: add the sstable component separator because for legacy (2.1) files
        // there is no separator after the generation number, and this would cause files of sstables with
        // a higher generation number that starts with the same number, to be incorrectly classified as files
        // of this record sstable
        String absoluteTablePath = absolutePath(table.descriptor.baseFilename());
        return make(type, getExistingFiles(absoluteTablePath), table.getAllFilePaths().size(), absoluteTablePath);
    }

    public static Map<SSTable, LogRecord> make(Type type, Iterable<SSTableReader> tables)
    {
        // contains a mapping from sstable absolute path (everything up until the 'Data'/'Index'/etc part of the filename) to the sstable
        Map<String, SSTable> absolutePaths = new HashMap<>();
        for (SSTableReader table : tables)
            absolutePaths.put(absolutePath(table.descriptor.baseFilename()), table);

        // maps sstable base file name to the actual files on disk
        Map<String, List<File>> existingFiles = getExistingFiles(absolutePaths.keySet());
        Map<SSTable, LogRecord> records = new HashMap<>(existingFiles.size());
        for (Map.Entry<String, List<File>> entry : existingFiles.entrySet())
        {
            List<File> filesOnDisk = entry.getValue();
            String baseFileName = entry.getKey();
            SSTable sstable = absolutePaths.get(baseFileName);
            records.put(sstable, make(type, filesOnDisk, sstable.getAllFilePaths().size(), baseFileName));
        }
        return records;
    }

    private static String absolutePath(String baseFilename)
    {
        return FileUtils.getCanonicalPath(baseFilename + Component.separator);
    }

    public LogRecord withExistingFiles()
    {
        return make(type, getExistingFiles(), 0, absolutePath.get());
    }

    public static LogRecord make(Type type, List<File> files, int minFiles, String absolutePath)
    {
        // CASSANDRA-11889: File.lastModified() returns a positive value only if the file exists, therefore
        // we filter by positive values to only consider the files that still exists right now, in case things
        // changed on disk since getExistingFiles() was called
        List<Long> positiveModifiedTimes = files.stream().map(File::lastModified).filter(lm -> lm > 0).collect(Collectors.toList());
        long lastModified = positiveModifiedTimes.stream().reduce(0L, Long::max);
        return new LogRecord(type, absolutePath, lastModified, Math.max(minFiles, positiveModifiedTimes.size()));
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
        this.absolutePath = type.hasFile() ? Optional.of(absolutePath) : Optional.empty();
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

    /**
     * absoluteFilePaths contains full file parts up to the component name
     *
     * this method finds all files on disk beginning with any of the paths in absoluteFilePaths
     * @return a map from absoluteFilePath to actual file on disk.
     */
    public static Map<String, List<File>> getExistingFiles(Set<String> absoluteFilePaths)
    {
        Set<File> uniqueDirectories = absoluteFilePaths.stream().map(path -> Paths.get(path).getParent().toFile()).collect(Collectors.toSet());
        Map<String, List<File>> fileMap = new HashMap<>();
        FilenameFilter ff = (dir, name) -> {
            Descriptor descriptor = null;
            try
            {
                descriptor = Descriptor.fromFilename(dir, name).left;
            }
            catch (Throwable t)
            {// ignored - if we can't parse the filename, just skip the file
            }

            String absolutePath = descriptor != null ? absolutePath(descriptor.baseFilename()) : null;
            if (absolutePath != null && absoluteFilePaths.contains(absolutePath))
                fileMap.computeIfAbsent(absolutePath, k -> new ArrayList<>()).add(new File(dir, name));

            return false;
        };

        // populate the file map:
        for (File f : uniqueDirectories)
            f.listFiles(ff);

        return fileMap;
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

    /**
     * Return the absolute path, if present, except for the last character (the descriptor separator), or
     * the empty string if the record has no path. This method is only to be used internally for writing
     * the record to file or computing the checksum.
     *
     * CASSANDRA-13294: the last character of the absolute path is the descriptor separator, it is removed
     * from the absolute path for backward compatibility, to make sure that on upgrade from 3.0.x to 3.0.y
     * or to 3.y or to 4.0, the checksum of existing txn files still matches (in case of non clean shutdown
     * some txn files may be present). By removing the last character here, it means that
     * it will never be written to txn files, but it is added after reading a txn file in LogFile.make().
     */
    private String absolutePath()
    {
        if (!absolutePath.isPresent())
            return "";

        String ret = absolutePath.get();
        assert ret.charAt(ret.length() -1) == Component.separator : "Invalid absolute path, should end with '-'";
        return ret.substring(0, ret.length() - 1);
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

    LogRecord asType(Type type)
    {
        return new LogRecord(type, absolutePath.orElse(null), updateTime, numFiles);
    }
}
