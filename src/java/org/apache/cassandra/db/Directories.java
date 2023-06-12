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
package org.apache.cassandra.db;

import java.io.IOError;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSNoDiskAvailableForWriteError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileStoreUtils;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.snapshot.SnapshotManifest;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.utils.DirectorySizeCalculator;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;

/**
 * Encapsulate handling of paths to the data files.
 *
 * <pre> {@code
 *   /<path_to_data_dir>/ks/<cf dir>/ks-cf1-jb-1-Data.db
 *                         /<cf dir>/la-2-Data.db
 *                         /<cf dir>/.<index name>/ks-cf1.idx-jb-1-Data.db
 *                         /<cf dir>/.<index name>/la-1-Data.db
 *                         ...
 * } </pre>
 *
 * Until v2.0, {@code <cf dir>} is just column family name.
 * Since v2.1, {@code <cf dir>} has column family ID(tableId) added to its end.
 *
 * SSTables from secondary indexes were put in the same directory as their parent.
 * Since v2.2, they have their own directory under the parent directory whose name is index name.
 * Upon startup, those secondary index files are moved to new directory when upgrading.
 *
 * For backward compatibility, Directories can use directory without tableId if exists.
 *
 * In addition, more that one 'root' data directory can be specified so that
 * {@code <path_to_data_dir>} potentially represents multiple locations.
 * Note that in the case of multiple locations, the manifest for the leveled
 * compaction is only in one of the location.
 *
 * Snapshots (resp. backups) are always created along the sstables there are
 * snapshotted (resp. backuped) but inside a subdirectory named 'snapshots'
 * (resp. backups) (and snapshots are further inside a subdirectory of the name
 * of the snapshot). For secondary indexes, snapshots (backups) are not created in
 * their own directory, but are in their parent's snapshot (backup) directory.
 *
 * This class abstracts all those details from the rest of the code.
 */
public class Directories
{
    private static final Logger logger = LoggerFactory.getLogger(Directories.class);

    public static final String BACKUPS_SUBDIR = "backups";
    public static final String SNAPSHOT_SUBDIR = "snapshots";
    public static final String TMP_SUBDIR = "tmp";
    public static final String SECONDARY_INDEX_NAME_SEPARATOR = ".";

    /**
     * The directories used to store keyspaces data.
     */
    public static final DataDirectories dataDirectories = new DataDirectories(DatabaseDescriptor.getNonLocalSystemKeyspacesDataFileLocations(),
                                                                              DatabaseDescriptor.getLocalSystemKeyspacesDataFileLocations());

    /**
     * Checks whether Cassandra has RWX permissions to the specified directory.  Logs an error with
     * the details if it does not.
     *
     * @param dir File object of the directory.
     * @param dataDir String representation of the directory's location
     * @return status representing Cassandra's RWX permissions to the supplied folder location.
     */
    public static boolean verifyFullPermissions(File dir, String dataDir)
    {
        if (!dir.isDirectory())
        {
            logger.error("Not a directory {}", dataDir);
            return false;
        }
        else if (!FileAction.hasPrivilege(dir, FileAction.X))
        {
            logger.error("Doesn't have execute permissions for {} directory", dataDir);
            return false;
        }
        else if (!FileAction.hasPrivilege(dir, FileAction.R))
        {
            logger.error("Doesn't have read permissions for {} directory", dataDir);
            return false;
        }
        else if (dir.exists() && !FileAction.hasPrivilege(dir, FileAction.W))
        {
            logger.error("Doesn't have write permissions for {} directory", dataDir);
            return false;
        }

        return true;
    }

    public enum FileAction
    {
        X, W, XW, R, XR, RW, XRW;

        FileAction()
        {
        }

        public static boolean hasPrivilege(File file, FileAction action)
        {
            boolean privilege = false;

            switch (action)
            {
                case X:
                    privilege = file.isExecutable();
                    break;
                case W:
                    privilege = file.isWritable();
                    break;
                case XW:
                    privilege = file.isExecutable() && file.isWritable();
                    break;
                case R:
                    privilege = file.isReadable();
                    break;
                case XR:
                    privilege = file.isExecutable() && file.isReadable();
                    break;
                case RW:
                    privilege = file.isReadable() && file.isWritable();
                    break;
                case XRW:
                    privilege = file.isExecutable() && file.isReadable() && file.isWritable();
                    break;
            }
            return privilege;
        }
    }

    private final TableMetadata metadata;
    private final DataDirectory[] paths;
    private final File[] dataPaths;
    private final ImmutableMap<Path, DataDirectory> canonicalPathToDD;

    public Directories(final TableMetadata metadata)
    {
        this(metadata, dataDirectories.getDataDirectoriesFor(metadata));
    }

    public Directories(final TableMetadata metadata, Collection<DataDirectory> paths)
    {
        this(metadata, paths.toArray(new DataDirectory[paths.size()]));
    }

    /**
     * Create Directories of given ColumnFamily.
     * SSTable directories are created under data_directories defined in cassandra.yaml if not exist at this time.
     *
     * @param metadata metadata of ColumnFamily
     */
    public Directories(final TableMetadata metadata, DataDirectory[] paths)
    {
        this.metadata = metadata;
        this.paths = paths;
        ImmutableMap.Builder<Path, DataDirectory> canonicalPathsBuilder = ImmutableMap.builder();
        String tableId = metadata.id.toHexString();
        int idx = metadata.name.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
        String cfName = idx >= 0 ? metadata.name.substring(0, idx) : metadata.name;
        String indexNameWithDot = idx >= 0 ? metadata.name.substring(idx) : null;

        this.dataPaths = new File[paths.length];
        // If upgraded from version less than 2.1, use existing directories
        String oldSSTableRelativePath = join(metadata.keyspace, cfName);
        for (int i = 0; i < paths.length; ++i)
        {
            // check if old SSTable directory exists
            File dataPath = new File(paths[i].location, oldSSTableRelativePath);
            dataPaths[i] = dataPath;
            canonicalPathsBuilder.put(dataPath.toCanonical().toPath(), paths[i]);
        }
        boolean olderDirectoryExists = Iterables.any(Arrays.asList(dataPaths), File::exists);
        if (!olderDirectoryExists)
        {
            canonicalPathsBuilder = ImmutableMap.builder();
            // use 2.1+ style
            String newSSTableRelativePath = join(metadata.keyspace, cfName + '-' + tableId);
            for (int i = 0; i < paths.length; ++i)
            {
                File dataPath = new File(paths[i].location, newSSTableRelativePath);
                dataPaths[i] = dataPath;
                canonicalPathsBuilder.put(dataPath.toCanonical().toPath(), paths[i]);
            }
        }
        // if index, then move to its own directory
        if (indexNameWithDot != null)
        {
            canonicalPathsBuilder = ImmutableMap.builder();
            for (int i = 0; i < paths.length; ++i)
            {
                File dataPath = new File(dataPaths[i], indexNameWithDot);
                dataPaths[i] = dataPath;
                canonicalPathsBuilder.put(dataPath.toCanonical().toPath(), paths[i]);
            }
        }

        for (File dir : dataPaths)
        {
            try
            {
                FileUtils.createDirectory(dir);
            }
            catch (FSError e)
            {
                // don't just let the default exception handler do this, we need the create loop to continue
                logger.error("Failed to create {} directory", dir);
                JVMStabilityInspector.inspectThrowable(e);
            }
        }

        // if index, move existing older versioned SSTable files to new directory
        if (indexNameWithDot != null)
        {
            for (File dataPath : dataPaths)
            {
                File[] indexFiles = dataPath.parent().tryList(file -> {
                    if (file.isDirectory())
                        return false;

                    Descriptor desc = SSTable.tryDescriptorFromFile(file);
                    return desc != null && desc.ksname.equals(metadata.keyspace) && desc.cfname.equals(metadata.name);
                });
                for (File indexFile : indexFiles)
                {
                    File destFile = new File(dataPath, indexFile.name());
                    logger.trace("Moving index file {} to {}", indexFile, destFile);
                    FileUtils.renameWithConfirm(indexFile, destFile);
                }
            }
        }
        canonicalPathToDD = canonicalPathsBuilder.build();
    }

    /**
     * Returns SSTable location which is inside given data directory.
     *
     * @param dataDirectory
     * @return SSTable location
     */
    public File getLocationForDisk(DataDirectory dataDirectory)
    {
        if (dataDirectory != null)
            for (File dir : dataPaths)
            {
                // Note that we must compare absolute paths (not canonical) here since keyspace directories might be symlinks
                Path dirPath = dir.toAbsolute().toPath();
                Path locationPath = dataDirectory.location.toAbsolute().toPath();
                if (dirPath.startsWith(locationPath))
                    return dir;
            }
        return null;
    }

    public DataDirectory getDataDirectoryForFile(Descriptor descriptor)
    {
        if (descriptor != null)
            return canonicalPathToDD.get(descriptor.directory.toPath());
        return null;
    }

    public Descriptor find(String filename)
    {
        for (File dir : dataPaths)
        {
            File file = new File(dir, filename);
            if (file.exists())
                return Descriptor.fromFileWithComponent(file, false).left;
        }
        return null;
    }

    /**
     * Basically the same as calling {@link #getWriteableLocationAsFile(long)} with an unknown size ({@code -1L}),
     * which may return any allowed directory - even a data directory that has no usable space.
     * Do not use this method in production code.
     *
     * @throws FSWriteError if all directories are disallowed.
     */
    public File getDirectoryForNewSSTables()
    {
        return getWriteableLocationAsFile(-1L);
    }

    /**
     * Returns an allowed directory that _currently_ has {@code writeSize} bytes as usable space.
     *
     * @throws FSWriteError if all directories are disallowed.
     */
    public File getWriteableLocationAsFile(long writeSize)
    {
        File location = getLocationForDisk(getWriteableLocation(writeSize));
        if (location == null)
            throw new FSWriteError(new IOException("No configured data directory contains enough space to write " + writeSize + " bytes"), "");
        return location;
    }

    /**
     * Returns a data directory to load the file {@code sourceFile}. If the sourceFile is on same disk partition as any
     * data directory then use that one as data directory otherwise use {@link #getWriteableLocationAsFile(long)} to
     * find suitable data directory.
     *
     * Also makes sure returned directory is not disallowed.
     *
     * @throws FSWriteError if all directories are disallowed.
     */
    public File getWriteableLocationToLoadFile(final File sourceFile)
    {
        try
        {
            final FileStore srcFileStore = Files.getFileStore(sourceFile.toPath());
            for (final File dataPath : dataPaths)
            {
                if (DisallowedDirectories.isUnwritable(dataPath))
                {
                    continue;
                }

                if (Files.getFileStore(dataPath.toPath()).equals(srcFileStore))
                {
                    return dataPath;
                }
            }
        }
        catch (final IOException e)
        {
            // pass exceptions in finding filestore. This is best effort anyway. Fall back on getWriteableLocationAsFile()
        }

        return getWriteableLocationAsFile(sourceFile.length());
    }

    /**
     * Returns a temporary subdirectory on allowed data directory
     * that _currently_ has {@code writeSize} bytes as usable space.
     * This method does not create the temporary directory.
     *
     * @throws IOError if all directories are disallowed.
     */
    public File getTemporaryWriteableDirectoryAsFile(long writeSize)
    {
        File location = getLocationForDisk(getWriteableLocation(writeSize));
        if (location == null)
            return null;
        return new File(location, TMP_SUBDIR);
    }

    public void removeTemporaryDirectories()
    {
        for (File dataDir : dataPaths)
        {
            File tmpDir = new File(dataDir, TMP_SUBDIR);
            if (tmpDir.exists())
            {
                logger.debug("Removing temporary directory {}", tmpDir);
                FileUtils.deleteRecursive(tmpDir);
            }
        }
    }

    /**
     * Returns an allowed data directory that _currently_ has {@code writeSize} bytes as usable space.
     *
     * @throws FSWriteError if all directories are disallowed.
     */
    public DataDirectory getWriteableLocation(long writeSize)
    {
        List<DataDirectoryCandidate> candidates = new ArrayList<>();

        long totalAvailable = 0L;

        // pick directories with enough space and so that resulting sstable dirs aren't disallowed for writes.
        boolean tooBig = false;
        for (DataDirectory dataDir : paths)
        {
            if (DisallowedDirectories.isUnwritable(getLocationForDisk(dataDir)))
            {
                logger.trace("removing disallowed candidate {}", dataDir.location);
                continue;
            }
            DataDirectoryCandidate candidate = new DataDirectoryCandidate(dataDir);
            // exclude directory if its total writeSize does not fit to data directory
            if (candidate.availableSpace < writeSize)
            {
                logger.trace("removing candidate {}, usable={}, requested={}", candidate.dataDirectory.location, candidate.availableSpace, writeSize);
                tooBig = true;
                continue;
            }
            candidates.add(candidate);
            totalAvailable += candidate.availableSpace;
        }

        if (candidates.isEmpty())
        {
            if (tooBig)
                throw new FSDiskFullWriteError(metadata.keyspace, writeSize);

            throw new FSNoDiskAvailableForWriteError(metadata.keyspace);
        }

        // shortcut for single data directory systems
        if (candidates.size() == 1)
            return candidates.get(0).dataDirectory;

        sortWriteableCandidates(candidates, totalAvailable);

        return pickWriteableDirectory(candidates);
    }

    // separated for unit testing
    static DataDirectory pickWriteableDirectory(List<DataDirectoryCandidate> candidates)
    {
        // weighted random
        double rnd = ThreadLocalRandom.current().nextDouble();
        for (DataDirectoryCandidate candidate : candidates)
        {
            rnd -= candidate.perc;
            if (rnd <= 0)
                return candidate.dataDirectory;
        }

        // last resort
        return candidates.get(0).dataDirectory;
    }

    // separated for unit testing
    static void sortWriteableCandidates(List<DataDirectoryCandidate> candidates, long totalAvailable)
    {
        // calculate free-space-percentage
        for (DataDirectoryCandidate candidate : candidates)
            candidate.calcFreePerc(totalAvailable);

        // sort directories by perc
        Collections.sort(candidates);
    }

    /**
     * Sums up the space required for ongoing streams + compactions + expected new write size per FileStore and checks
     * if there is enough space available.
     *
     * @param expectedNewWriteSizes where we expect to write the new compactions
     * @param totalCompactionWriteRemaining approximate amount of data current compactions are writing - keyed by
     *                                      the file store they are writing to (or, reading from actually, but since
     *                                      CASSANDRA-6696 we expect compactions to read and written from the same dir)
     * @return true if we expect to be able to write expectedNewWriteSizes to the available file stores
     */
    public boolean hasDiskSpaceForCompactionsAndStreams(Map<File, Long> expectedNewWriteSizes,
                                                        Map<File, Long> totalCompactionWriteRemaining)
    {
        return hasDiskSpaceForCompactionsAndStreams(expectedNewWriteSizes, totalCompactionWriteRemaining, Directories::getFileStore);
    }

    @VisibleForTesting
    public static boolean hasDiskSpaceForCompactionsAndStreams(Map<File, Long> expectedNewWriteSizes,
                                                               Map<File, Long> totalCompactionWriteRemaining,
                                                               Function<File, FileStore> filestoreMapper)
    {
        Map<FileStore, Long> newWriteSizesPerFileStore = perFileStore(expectedNewWriteSizes, filestoreMapper);
        Map<FileStore, Long> compactionsRemainingPerFileStore = perFileStore(totalCompactionWriteRemaining, filestoreMapper);

        Map<FileStore, Long> totalPerFileStore = new HashMap<>();
        for (Map.Entry<FileStore, Long> entry : newWriteSizesPerFileStore.entrySet())
        {
            long addedForFilestore = entry.getValue() + compactionsRemainingPerFileStore.getOrDefault(entry.getKey(), 0L);
            totalPerFileStore.merge(entry.getKey(), addedForFilestore, Long::sum);
        }
        return hasDiskSpaceForCompactionsAndStreams(totalPerFileStore);
    }

    /**
     * Checks if there is enough space on all file stores to write the given amount of data.
     * The data to write should be the total amount, ongoing writes + new writes.
     */
    public static boolean hasDiskSpaceForCompactionsAndStreams(Map<FileStore, Long> totalToWrite)
    {
        boolean hasSpace = true;
        for (Map.Entry<FileStore, Long> toWrite : totalToWrite.entrySet())
        {
            long availableForCompaction = getAvailableSpaceForCompactions(toWrite.getKey());
            logger.debug("FileStore {} has {} bytes available, checking if we can write {} bytes", toWrite.getKey(), availableForCompaction, toWrite.getValue());
            if (availableForCompaction < toWrite.getValue())
            {
                logger.warn("FileStore {} has only {} available, but {} is needed",
                            toWrite.getKey(),
                            FileUtils.stringifyFileSize(availableForCompaction),
                            FileUtils.stringifyFileSize((long) toWrite.getValue()));
                hasSpace = false;
            }
        }
        return hasSpace;
    }

    public static long getAvailableSpaceForCompactions(FileStore fileStore)
    {
        long availableSpace = 0;
        availableSpace = FileStoreUtils.tryGetSpace(fileStore, FileStore::getUsableSpace, e -> { throw new FSReadError(e, fileStore.name()); })
                         - DatabaseDescriptor.getMinFreeSpacePerDriveInBytes();
        return Math.max(0L, Math.round(availableSpace * DatabaseDescriptor.getMaxSpaceForCompactionsPerDrive()));
    }

    public static Map<FileStore, Long> perFileStore(Map<File, Long> perDirectory, Function<File, FileStore> filestoreMapper)
    {
        return perDirectory.entrySet()
                           .stream()
                           .collect(Collectors.toMap(entry -> filestoreMapper.apply(entry.getKey()),
                                                     Map.Entry::getValue,
                                                     Long::sum));
    }

    public Set<FileStore> allFileStores(Function<File, FileStore> filestoreMapper)
    {
        return Arrays.stream(getWriteableLocations())
                     .map(this::getLocationForDisk)
                     .map(filestoreMapper)
                     .collect(Collectors.toSet());
    }

    /**
     * Gets the filestore for the actual directory where the sstables are stored.
     * Handles the fact that an operator can symlink a table directory to a different filestore.
     */
    public static FileStore getFileStore(File directory)
    {
        try
        {
            return Files.getFileStore(directory.toPath());
        }
        catch (IOException e)
        {
            throw new FSReadError(e, directory);
        }
    }

    public DataDirectory[] getWriteableLocations()
    {
        List<DataDirectory> allowedDirs = new ArrayList<>(paths.length);
        for (DataDirectory dir : paths)
        {
            if (!DisallowedDirectories.isUnwritable(dir.location))
                allowedDirs.add(dir);
        }

        if (allowedDirs.isEmpty())
            throw new FSNoDiskAvailableForWriteError(metadata.keyspace);

        allowedDirs.sort(Comparator.comparing(o -> o.location));
        return allowedDirs.toArray(new DataDirectory[allowedDirs.size()]);
    }

    public static File getSnapshotDirectory(Descriptor desc, String snapshotName)
    {
        return getSnapshotDirectory(desc.directory, snapshotName);
    }

    /**
     * Returns directory to write snapshot. If directory does not exist, then one is created.
     *
     * If given {@code location} indicates secondary index, this will return
     * {@code <cf dir>/snapshots/<snapshot name>/.<index name>}.
     * Otherwise, this will return {@code <cf dir>/snapshots/<snapshot name>}.
     *
     * @param location base directory
     * @param snapshotName snapshot name
     * @return directory to write snapshot
     */
    public static File getSnapshotDirectory(File location, String snapshotName)
    {
        if (isSecondaryIndexFolder(location))
        {
            return getOrCreate(location.parent(), SNAPSHOT_SUBDIR, snapshotName, location.name());
        }
        else
        {
            return getOrCreate(location, SNAPSHOT_SUBDIR, snapshotName);
        }
    }

    public File getSnapshotManifestFile(String snapshotName)
    {
        File snapshotDir = getSnapshotDirectory(getDirectoryForNewSSTables(), snapshotName);
        return getSnapshotManifestFile(snapshotDir);
    }

    public static File getSnapshotManifestFile(File snapshotDir)
    {
        return new File(snapshotDir, "manifest.json");
    }

    public File getSnapshotSchemaFile(String snapshotName)
    {
        File snapshotDir = getSnapshotDirectory(getDirectoryForNewSSTables(), snapshotName);
        return getSnapshotSchemaFile(snapshotDir);
    }

    public static File getSnapshotSchemaFile(File snapshotDir)
    {
        return new File(snapshotDir, "schema.cql");
    }

    public static File getBackupsDirectory(Descriptor desc)
    {
        return getBackupsDirectory(desc.directory);
    }

    public static File getBackupsDirectory(File location)
    {
        if (isSecondaryIndexFolder(location))
        {
            return getOrCreate(location.parent(), BACKUPS_SUBDIR, location.name());
        }
        else
        {
            return getOrCreate(location, BACKUPS_SUBDIR);
        }
    }

    /**
     * Checks if the specified table should be stored with local system data.
     *
     * <p> To minimize the risk of failures, SSTables for local system keyspaces must be stored in a single data
     * directory. The only exception to this are some of the system table as the server can continue operating even
     *  if those tables loose some data.</p>
     *
     * @param keyspace the keyspace name
     * @param table the table name
     * @return {@code true} if the specified table should be stored with local system data, {@code false} otherwise.
     */
    public static boolean isStoredInLocalSystemKeyspacesDataLocation(String keyspace, String table)
    {
        String keyspaceName = keyspace.toLowerCase();

        return SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES.contains(keyspaceName)
                && !(SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(keyspaceName)
                        && SystemKeyspace.TABLES_SPLIT_ACROSS_MULTIPLE_DISKS.contains(table.toLowerCase()));
    }

    public static class DataDirectory
    {
        public final File location;

        public DataDirectory(String location)
        {
            this(new File(location));
        }

        public DataDirectory(File location)
        {
            this.location = location;
        }

        public DataDirectory(Path location)
        {
            this.location = new File(location);
        }

        public long getAvailableSpace()
        {
            long availableSpace = PathUtils.tryGetSpace(location.toPath(), FileStore::getUsableSpace) - DatabaseDescriptor.getMinFreeSpacePerDriveInBytes();
            return availableSpace > 0 ? availableSpace : 0;
        }

        public long getRawSize()
        {
            return FileUtils.folderSize(location);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DataDirectory that = (DataDirectory) o;

            return location.equals(that.location);
        }

        @Override
        public int hashCode()
        {
            return location.hashCode();
        }

        public String toString()
        {
            return "DataDirectory{" +
                   "location=" + location +
                   '}';
        }
    }

    /**
     * Data directories used to store keyspace data.
     */
    public static final class DataDirectories implements Iterable<DataDirectory>
    {
        /**
         * The directories for storing the local system keyspaces.
         */
        private final DataDirectory[] localSystemKeyspaceDataDirectories;

        /**
         * The directories where the data of the non local system keyspaces should be stored.
         */
        private final DataDirectory[] nonLocalSystemKeyspacesDirectories;


        public DataDirectories(String[] locationsForNonSystemKeyspaces, String[] locationsForSystemKeyspace)
        {
            nonLocalSystemKeyspacesDirectories = toDataDirectories(locationsForNonSystemKeyspaces);
            localSystemKeyspaceDataDirectories = toDataDirectories(locationsForSystemKeyspace);
        }

        private static DataDirectory[] toDataDirectories(String... locations)
        {
            DataDirectory[] directories = new DataDirectory[locations.length];
            for (int i = 0; i < locations.length; ++i)
                directories[i] = new DataDirectory(new File(locations[i]));
            return directories;
        }

        /**
         * Returns the data directories for the specified table.
         *
         * @param table the table metadata
         * @return the data directories for the specified table
         */
        public DataDirectory[] getDataDirectoriesFor(TableMetadata table)
        {
            return isStoredInLocalSystemKeyspacesDataLocation(table.keyspace, table.name) ? localSystemKeyspaceDataDirectories
                                                                                          : nonLocalSystemKeyspacesDirectories;
        }

        @Override
        public Iterator<DataDirectory> iterator()
        {
            return getAllDirectories().iterator();
        }

        public Set<DataDirectory> getAllDirectories()
        {
            Set<DataDirectory> directories = new LinkedHashSet<>(nonLocalSystemKeyspacesDirectories.length + localSystemKeyspaceDataDirectories.length);
            Collections.addAll(directories, nonLocalSystemKeyspacesDirectories);
            Collections.addAll(directories, localSystemKeyspaceDataDirectories);
            return directories;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DataDirectories that = (DataDirectories) o;

            return Arrays.equals(this.localSystemKeyspaceDataDirectories, that.localSystemKeyspaceDataDirectories)
                && Arrays.equals(this.nonLocalSystemKeyspacesDirectories, that.nonLocalSystemKeyspacesDirectories);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(localSystemKeyspaceDataDirectories, nonLocalSystemKeyspacesDirectories);
        }

        @Override
        public String toString()
        {
            return "DataDirectories {" +
                   "systemKeyspaceDataDirectories=" + Arrays.toString(localSystemKeyspaceDataDirectories) +
                   ", nonSystemKeyspacesDirectories=" + Arrays.toString(nonLocalSystemKeyspacesDirectories) +
                   '}';
        }
    }

    static final class DataDirectoryCandidate implements Comparable<DataDirectoryCandidate>
    {
        final DataDirectory dataDirectory;
        final long availableSpace;
        double perc;

        public DataDirectoryCandidate(DataDirectory dataDirectory)
        {
            this.dataDirectory = dataDirectory;
            this.availableSpace = dataDirectory.getAvailableSpace();
        }

        void calcFreePerc(long totalAvailableSpace)
        {
            double w = availableSpace;
            w /= totalAvailableSpace;
            perc = w;
        }

        public int compareTo(DataDirectoryCandidate o)
        {
            if (this == o)
                return 0;

            int r = Double.compare(perc, o.perc);
            if (r != 0)
                return -r;
            // last resort
            return System.identityHashCode(this) - System.identityHashCode(o);
        }

        @Override
        public String toString()
        {
            return "DataDirectoryCandidate{" +
                   "dataDirectory=" + dataDirectory +
                   ", availableSpace=" + availableSpace +
                   ", perc=" + perc +
                   '}';
        }
    }

    /** The type of files that can be listed by SSTableLister, we never return txn logs,
     * use LifecycleTransaction.getFiles() if you need txn logs. */
    public enum FileType
    {
        /** A permanent sstable file that is safe to use. */
        FINAL,

        /** A temporary sstable file that will soon be deleted. */
        TEMPORARY,

        /** A transaction log file (contains information on final and temporary files). */
        TXN_LOG;
    }

    /**
     * How to handle a failure to read a txn log file. Note that we will try a few
     * times before giving up.
     **/
    public enum OnTxnErr
    {
        /** Throw the exception */
        THROW,

        /** Ignore the problematic parts of the txn log file */
        IGNORE
    }

    public SSTableLister sstableLister(OnTxnErr onTxnErr)
    {
        return new SSTableLister(this.dataPaths, this.metadata, onTxnErr);
    }

    public SSTableLister sstableLister(File directory, OnTxnErr onTxnErr)
    {
        return new SSTableLister(new File[]{directory}, metadata, onTxnErr);
    }

    public static class SSTableLister
    {
        private final OnTxnErr onTxnErr;
        private boolean skipTemporary;
        private boolean includeBackups;
        private boolean onlyBackups;
        private int nbFiles;
        private final Map<Descriptor, Set<Component>> components = new HashMap<>();
        private boolean filtered;
        private String snapshotName;
        private final File[] dataPaths;
        private final TableMetadata metadata;

        private SSTableLister(File[] dataPaths, TableMetadata metadata, OnTxnErr onTxnErr)
        {
            this.dataPaths = dataPaths;
            this.metadata = metadata;
            this.onTxnErr = onTxnErr;
        }

        public SSTableLister skipTemporary(boolean b)
        {
            if (filtered)
                throw new IllegalStateException("list() has already been called");
            skipTemporary = b;
            return this;
        }

        public SSTableLister includeBackups(boolean b)
        {
            if (filtered)
                throw new IllegalStateException("list() has already been called");
            includeBackups = b;
            return this;
        }

        public SSTableLister onlyBackups(boolean b)
        {
            if (filtered)
                throw new IllegalStateException("list() has already been called");
            onlyBackups = b;
            includeBackups = b;
            return this;
        }

        public SSTableLister snapshots(String sn)
        {
            if (filtered)
                throw new IllegalStateException("list() has already been called");
            snapshotName = sn;
            return this;
        }

        public Map<Descriptor, Set<Component>> list()
        {
            filter();
            return ImmutableMap.copyOf(components);
        }

        /**
         * Returns a sorted version of the {@code list} method.
         * Descriptors are sorted by generation.
         * @return a List of descriptors to their components.
         */
        public List<Map.Entry<Descriptor, Set<Component>>> sortedList()
        {
            List<Map.Entry<Descriptor, Set<Component>>> sortedEntries = new ArrayList<>(list().entrySet());
            sortedEntries.sort((o1, o2) -> SSTableIdFactory.COMPARATOR.compare(o1.getKey().id, o2.getKey().id));
            return sortedEntries;
        }

        public List<File> listFiles()
        {
            filter();
            List<File> l = new ArrayList<>(nbFiles);
            for (Map.Entry<Descriptor, Set<Component>> entry : components.entrySet())
            {
                for (Component c : entry.getValue())
                {
                    l.add(entry.getKey().fileFor(c));
                }
            }
            return l;
        }

        private void filter()
        {
            if (filtered)
                return;

            for (File location : dataPaths)
            {
                if (DisallowedDirectories.isUnreadable(location))
                    continue;

                if (snapshotName != null)
                {
                    LifecycleTransaction.getFiles(getSnapshotDirectory(location, snapshotName).toPath(), getFilter(), onTxnErr);
                    continue;
                }

                if (!onlyBackups)
                    LifecycleTransaction.getFiles(location.toPath(), getFilter(), onTxnErr);

                if (includeBackups)
                    LifecycleTransaction.getFiles(getBackupsDirectory(location).toPath(), getFilter(), onTxnErr);
            }

            filtered = true;
        }

        private BiPredicate<File, FileType> getFilter()
        {
            // This function always return false since it adds to the components map
            return (file, type) ->
            {
                switch (type)
                {
                    case TXN_LOG:
                        return false;
                    case TEMPORARY:
                        if (skipTemporary)
                            return false;

                    case FINAL:
                        Pair<Descriptor, Component> pair = SSTable.tryComponentFromFilename(file);
                        if (pair == null)
                            return false;

                        // we are only interested in the SSTable files that belong to the specific ColumnFamily
                        if (!pair.left.ksname.equals(metadata.keyspace) || !pair.left.cfname.equals(metadata.name))
                            return false;

                        Set<Component> previous = components.get(pair.left);
                        if (previous == null)
                        {
                            previous = new HashSet<>();
                            components.put(pair.left, previous);
                        }
                        previous.add(pair.right);
                        nbFiles++;
                        return false;

                    default:
                        throw new AssertionError();
                }
            };
        }
    }

    public Map<String, TableSnapshot> listSnapshots()
    {
        Map<String, Set<File>> snapshotDirsByTag = listSnapshotDirsByTag();

        Map<String, TableSnapshot> snapshots = Maps.newHashMapWithExpectedSize(snapshotDirsByTag.size());

        for (Map.Entry<String, Set<File>> entry : snapshotDirsByTag.entrySet())
        {
            String tag = entry.getKey();
            Set<File> snapshotDirs = entry.getValue();
            SnapshotManifest manifest = maybeLoadManifest(metadata.keyspace, metadata.name, tag, snapshotDirs);
            snapshots.put(tag, buildSnapshot(tag, manifest, snapshotDirs));
        }

        return snapshots;
    }

    private TableSnapshot buildSnapshot(String tag, SnapshotManifest manifest, Set<File> snapshotDirs)
    {
        boolean ephemeral = manifest != null ? manifest.isEphemeral() : isLegacyEphemeralSnapshot(snapshotDirs);
        Instant createdAt = manifest == null ? null : manifest.createdAt;
        Instant expiresAt = manifest == null ? null : manifest.expiresAt;
        return new TableSnapshot(metadata.keyspace, metadata.name, metadata.id.asUUID(), tag, createdAt, expiresAt,
                                 snapshotDirs, ephemeral);
    }

    private static boolean isLegacyEphemeralSnapshot(Set<File> snapshotDirs)
    {
        return snapshotDirs.stream().map(d -> new File(d, "ephemeral.snapshot")).anyMatch(File::exists);
    }

    @VisibleForTesting
    protected static SnapshotManifest maybeLoadManifest(String keyspace, String table, String tag, Set<File> snapshotDirs)
    {
        List<File> manifests = snapshotDirs.stream().map(d -> new File(d, "manifest.json"))
                                           .filter(File::exists).collect(Collectors.toList());

        if (manifests.isEmpty())
        {
            logger.warn("No manifest found for snapshot {} of table {}.{}.", tag, keyspace, table);
            return null;
        }

        if (manifests.size() > 1) {
            logger.warn("Found multiple manifests for snapshot {} of table {}.{}", tag, keyspace, table);
        }

        try
        {
            return SnapshotManifest.deserializeFromJsonFile(manifests.get(0));
        }
        catch (IOException e)
        {
            logger.warn("Cannot read manifest file {} of snapshot {}.", manifests, tag, e);
        }

        return null;
    }

    @VisibleForTesting
    protected Map<String, Set<File>> listSnapshotDirsByTag()
    {
        Map<String, Set<File>> snapshotDirsByTag = new HashMap<>();
        for (final File dir : dataPaths)
        {
            File snapshotDir = isSecondaryIndexFolder(dir)
                               ? new File(dir.parentPath(), SNAPSHOT_SUBDIR)
                               : new File(dir, SNAPSHOT_SUBDIR);
            if (snapshotDir.exists() && snapshotDir.isDirectory())
            {
                final File[] snapshotDirs  = snapshotDir.tryList();
                if (snapshotDirs != null)
                {
                    for (final File snapshot : snapshotDirs)
                    {
                        if (snapshot.isDirectory()) {
                            snapshotDirsByTag.computeIfAbsent(snapshot.name(), k -> new LinkedHashSet<>()).add(snapshot.toAbsolute());
                        }
                    }
                }
            }
        }
        return snapshotDirsByTag;
    }

    public boolean snapshotExists(String snapshotName)
    {
        for (File dir : dataPaths)
        {
            File snapshotDir;
            if (isSecondaryIndexFolder(dir))
            {
                snapshotDir = new File(dir.parent(), join(SNAPSHOT_SUBDIR, snapshotName, dir.name()));
            }
            else
            {
                snapshotDir = new File(dir, join(SNAPSHOT_SUBDIR, snapshotName));
            }
            if (snapshotDir.exists())
                return true;
        }
        return false;
    }

    public static void clearSnapshot(String snapshotName, List<File> tableDirectories, RateLimiter snapshotRateLimiter)
    {
        // If snapshotName is empty or null, we will delete the entire snapshot directory
        String tag = snapshotName == null ? "" : snapshotName;
        for (File tableDir : tableDirectories)
        {
            File snapshotDir = new File(tableDir, join(SNAPSHOT_SUBDIR, tag));
            removeSnapshotDirectory(snapshotRateLimiter, snapshotDir);
        }
    }

    public static void removeSnapshotDirectory(RateLimiter snapshotRateLimiter, File snapshotDir)
    {
        if (snapshotDir.exists())
        {
            logger.trace("Removing snapshot directory {}", snapshotDir);
            try
            {
                FileUtils.deleteRecursiveWithThrottle(snapshotDir, snapshotRateLimiter);
            }
            catch (RuntimeException ex)
            {
                if (!snapshotDir.exists())
                    return; // ignore
                throw ex;
            }
        }
    }

    /**
     * @return total snapshot size in byte for all snapshots.
     */
    public long trueSnapshotsSize()
    {
        long result = 0L;
        for (File dir : dataPaths)
        {
            File snapshotDir = isSecondaryIndexFolder(dir)
                               ? new File(dir.parentPath(), SNAPSHOT_SUBDIR)
                               : new File(dir, SNAPSHOT_SUBDIR);
            result += getTrueAllocatedSizeIn(snapshotDir);
        }
        return result;
    }

    /**
     * @return Raw size on disk for all directories
     */
    public long getRawDiretoriesSize()
    {
        long totalAllocatedSize = 0L;

        for (File path : dataPaths)
            totalAllocatedSize += FileUtils.folderSize(path);

        return totalAllocatedSize;
    }

    public long getTrueAllocatedSizeIn(File snapshotDir)
    {
        if (!snapshotDir.isDirectory())
            return 0;

        SSTableSizeSummer visitor = new SSTableSizeSummer(sstableLister(OnTxnErr.THROW).listFiles());
        try
        {
            Files.walkFileTree(snapshotDir.toPath(), visitor);
        }
        catch (IOException e)
        {
            logger.error("Could not calculate the size of {}. {}", snapshotDir, e.getMessage());
        }

        return visitor.getAllocatedSize();
    }

    // Recursively finds all the sub directories in the KS directory.
    public static List<File> getKSChildDirectories(String ksName)
    {
        List<File> result = new ArrayList<>();
        for (DataDirectory dataDirectory : dataDirectories.getAllDirectories())
        {
            File ksDir = new File(dataDirectory.location, ksName);
            File[] cfDirs = ksDir.tryList();
            if (cfDirs == null)
                continue;
            for (File cfDir : cfDirs)
            {
                if (cfDir.isDirectory())
                    result.add(cfDir);
            }
        }
        return result;
    }

    public static boolean isSecondaryIndexFolder(File dir)
    {
        return dir.name().startsWith(SECONDARY_INDEX_NAME_SEPARATOR);
    }

    public static boolean isSecondaryIndexFolder(Path dir)
    {
        return PathUtils.filename(dir).startsWith(SECONDARY_INDEX_NAME_SEPARATOR);
    }

    public List<File> getCFDirectories()
    {
        List<File> result = new ArrayList<>();
        for (File dataDirectory : dataPaths)
        {
            if (dataDirectory.isDirectory())
                result.add(dataDirectory);
        }
        return result;
    }

    /**
     * Initializes the sstable unique identifier generator using a provided builder for this instance of directories.
     * If the id builder needs that, sstables in these directories are listed to provide the existing identifiers to
     * the builder. The listing is done lazily so if the builder does not require that, listing is skipped.
     */
    public <T extends SSTableId> Supplier<T> getUIDGenerator(SSTableId.Builder<T> builder)
    {
        // this stream is evaluated lazily - if the generator does not need the existing ids, we do not even call #sstableLister
        Stream<SSTableId> curIds = StreamSupport.stream(() -> sstableLister(Directories.OnTxnErr.IGNORE)
                                                              .includeBackups(true)
                                                              .list()
                                                              .keySet()
                                                              .spliterator(), Spliterator.DISTINCT, false)
                                                .map(d -> d.id);

        return builder.generator(curIds);
    }

    private static File getOrCreate(File base, String... subdirs)
    {
        File dir = subdirs == null || subdirs.length == 0 ? base : new File(base, join(subdirs));
        if (dir.exists())
        {
            if (!dir.isDirectory())
                throw new AssertionError(String.format("Invalid directory path %s: path exists but is not a directory", dir));
        }
        else if (!dir.tryCreateDirectories() && !(dir.exists() && dir.isDirectory()))
        {
            throw new FSWriteError(new IOException("Unable to create directory " + dir), dir);
        }
        return dir;
    }

    private static String join(String... s)
    {
        return StringUtils.join(s, File.pathSeparator());
    }

    private class SSTableSizeSummer extends DirectorySizeCalculator
    {
        private final Set<String> toSkip;
        SSTableSizeSummer(List<File> files)
        {
            toSkip = files.stream().map(File::name).collect(Collectors.toSet());
        }

        @Override
        public boolean isAcceptable(Path path)
        {
            File file = new File(path);
            Descriptor desc = SSTable.tryDescriptorFromFile(file);
            return desc != null
                && desc.ksname.equals(metadata.keyspace)
                && desc.cfname.equals(metadata.name)
                && !toSkip.contains(file.name());
        }
    }

}
