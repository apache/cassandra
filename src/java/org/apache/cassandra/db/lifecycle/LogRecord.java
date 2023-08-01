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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
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
        String absoluteTablePath = absolutePath(table.descriptor.baseFile());
        return make(type, getExistingFiles(absoluteTablePath), table.getAllFilePaths().size(), absoluteTablePath);
    }

    public static Map<SSTable, LogRecord> make(Type type, Iterable<SSTableReader> tables)
    {
        // contains a mapping from sstable absolute path (everything up until the 'Data'/'Index'/etc part of the filename) to the sstable
        Map<String, SSTable> absolutePaths = new HashMap<>();
        for (SSTableReader table : tables)
            absolutePaths.put(absolutePath(table.descriptor.baseFile()), table);

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

    private static String absolutePath(File baseFile)
    {
        return baseFile.withSuffix(String.valueOf(Component.separator)).canonicalPath();
    }

    public LogRecord withExistingFiles(List<File> existingFiles)
    {
        return make(type, existingFiles, 0, absolutePath.get());
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

    public static List<File> getExistingFiles(String absoluteFilePath)
    {
        File file = new File(absoluteFilePath);
        File[] files = file.parent().tryList((dir, name) -> name.startsWith(file.name()));
        // files may be null if the directory does not exist yet, e.g. when tracking new files
        return files == null ? Collections.emptyList() : Arrays.asList(files);
    }

    /**
     * absoluteFilePaths contains full file parts up to (but excluding) the component name
     *
     * This method finds all files on disk beginning with any of the paths in absoluteFilePaths
     *
     * @return a map from absoluteFilePath to actual file on disk.
     */
    public static Map<String, List<File>> getExistingFiles(Set<String> absoluteFilePaths)
    {
        Map<String, List<File>> fileMap = new HashMap<>();
        Map<File, TreeSet<String>> dirToFileNamePrefix = new HashMap<>();
        for (String absolutePath : absoluteFilePaths)
        {
            Path fullPath = new File(absolutePath).toPath();
            Path path = fullPath.getParent();
            if (path != null)
                dirToFileNamePrefix.computeIfAbsent(new File(path), (k) -> new TreeSet<>()).add(fullPath.getFileName().toString());
        }

        BiPredicate<File, String> ff = (dir, name) -> {
            TreeSet<String> dirSet = dirToFileNamePrefix.get(dir);
            // if the set contains a prefix of the current file name, the file name we have here should sort directly
            // after the prefix in the tree set, which means we can use 'floor' to get the prefix (returns the largest
            // of the smaller strings in the set). Also note that the prefixes always end with '-' which means we won't
            // have "xy-1111-Data.db".startsWith("xy-11") below (we'd get "xy-1111-Data.db".startsWith("xy-11-"))
            String baseName = dirSet.floor(name);
            if (baseName != null && name.startsWith(baseName))
            {
                String absolutePath = new File(dir, baseName).path();
                fileMap.computeIfAbsent(absolutePath, k -> new ArrayList<>()).add(new File(dir, name));
            }
            return false;
        };

        // populate the file map:
        for (File f : dirToFileNamePrefix.keySet())
            f.tryList(ff);

        return fileMap;
    }


    public boolean isFinal()
    {
        return type.isFinal();
    }

    String fileName()
    {
        return absolutePath.isPresent() ? new File(absolutePath.get()).name() : "";
    }

    boolean isInFolder(Path folder)
    {
        return absolutePath.isPresent() && PathUtils.isContained(folder, new File(absolutePath.get()).toPath());
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

    LogRecord asType(Type type)
    {
        return new LogRecord(type, absolutePath.orElse(null), updateTime, numFiles);
    }
}
