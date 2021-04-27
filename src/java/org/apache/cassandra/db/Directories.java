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

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.SnapshotDeletingTask;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.DirectorySizeCalculator;
import org.apache.cassandra.utils.FBUtilities;
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
                    privilege = file.canExecute();
                    break;
                case W:
                    privilege = file.canWrite();
                    break;
                case XW:
                    privilege = file.canExecute() && file.canWrite();
                    break;
                case R:
                    privilege = file.canRead();
                    break;
                case XR:
                    privilege = file.canExecute() && file.canRead();
                    break;
                case RW:
                    privilege = file.canRead() && file.canWrite();
                    break;
                case XRW:
                    privilege = file.canExecute() && file.canRead() && file.canWrite();
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
            canonicalPathsBuilder.put(Paths.get(FileUtils.getCanonicalPath(dataPath)), paths[i]);
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
                canonicalPathsBuilder.put(Paths.get(FileUtils.getCanonicalPath(dataPath)), paths[i]);
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
                canonicalPathsBuilder.put(Paths.get(FileUtils.getCanonicalPath(dataPath)), paths[i]);
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
                File[] indexFiles = dataPath.getParentFile().listFiles(new FileFilter()
                {
                    @Override
                    public boolean accept(File file)
                    {
                        if (file.isDirectory())
                            return false;

                        Descriptor desc = SSTable.tryDescriptorFromFilename(file);
                        return desc != null && desc.ksname.equals(metadata.keyspace) && desc.cfname.equals(metadata.name);

                    }
                });
                for (File indexFile : indexFiles)
                {
                    File destFile = new File(dataPath, indexFile.getName());
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
                Path dirPath = Paths.get(dir.getAbsolutePath());
                Path locationPath = Paths.get(dataDirectory.location.getAbsolutePath());
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
                return Descriptor.fromFilename(file);
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

    public boolean hasAvailableDiskSpace(long estimatedSSTables, long expectedTotalWriteSize)
    {
        long writeSize = expectedTotalWriteSize / estimatedSSTables;
        long totalAvailable = 0L;

        for (DataDirectory dataDir : paths)
        {
            if (DisallowedDirectories.isUnwritable(getLocationForDisk(dataDir)))
                  continue;
            DataDirectoryCandidate candidate = new DataDirectoryCandidate(dataDir);
            // exclude directory if its total writeSize does not fit to data directory
            logger.debug("DataDirectory {} has {} bytes available, checking if we can write {} bytes", dataDir.location, candidate.availableSpace, writeSize);
            if (candidate.availableSpace < writeSize)
            {
                logger.warn("DataDirectory {} can't be used for compaction. Only {} is available, but {} is the minimum write size.",
                            candidate.dataDirectory.location,
                            FileUtils.stringifyFileSize(candidate.availableSpace),
                            FileUtils.stringifyFileSize(writeSize));
                continue;
            }
            totalAvailable += candidate.availableSpace;
        }

        if (totalAvailable <= expectedTotalWriteSize)
        {
            StringJoiner pathString = new StringJoiner(",", "[", "]");
            for (DataDirectory p: paths)
            {
                pathString.add(p.location.getAbsolutePath());
            }
            logger.warn("Insufficient disk space for compaction. Across {} there's only {} available, but {} is needed.",
                        pathString.toString(),
                        FileUtils.stringifyFileSize(totalAvailable),
                        FileUtils.stringifyFileSize(expectedTotalWriteSize));
            return false;
        }
        return true;
    }

    public DataDirectory[] getWriteableLocations()
    {
        List<DataDirectory> allowedDirs = new ArrayList<>();
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
            return getOrCreate(location.getParentFile(), SNAPSHOT_SUBDIR, snapshotName, location.getName());
        }
        else
        {
            return getOrCreate(location, SNAPSHOT_SUBDIR, snapshotName);
        }
    }

    public File getSnapshotManifestFile(String snapshotName)
    {
        File snapshotDir = getSnapshotDirectory(getDirectoryForNewSSTables(), snapshotName);
        return new File(snapshotDir, "manifest.json");
    }

    public File getSnapshotSchemaFile(String snapshotName)
    {
        File snapshotDir = getSnapshotDirectory(getDirectoryForNewSSTables(), snapshotName);
        return new File(snapshotDir, "schema.cql");
    }

    public File getNewEphemeralSnapshotMarkerFile(String snapshotName)
    {
        File snapshotDir = new File(getWriteableLocationAsFile(1L), join(SNAPSHOT_SUBDIR, snapshotName));
        return getEphemeralSnapshotMarkerFile(snapshotDir);
    }

    private static File getEphemeralSnapshotMarkerFile(File snapshotDirectory)
    {
        return new File(snapshotDirectory, "ephemeral.snapshot");
    }

    public static File getBackupsDirectory(Descriptor desc)
    {
        return getBackupsDirectory(desc.directory);
    }

    public static File getBackupsDirectory(File location)
    {
        if (isSecondaryIndexFolder(location))
        {
            return getOrCreate(location.getParentFile(), BACKUPS_SUBDIR, location.getName());
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

        public long getAvailableSpace()
        {
            long availableSpace = FileUtils.getUsableSpace(location) - DatabaseDescriptor.getMinFreeSpacePerDriveInBytes();
            return availableSpace > 0 ? availableSpace : 0;
        }

        public long getTotalSpace()
        {
            return FileUtils.getTotalSpace(location);
        }

        public long getSpaceUsed()
        {
            return getTotalSpace() - getAvailableSpace();
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
            sortedEntries.sort(Comparator.comparing(t -> t.getKey().id, SSTableIdFactory.COMPARATOR));
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
                    l.add(new File(entry.getKey().filenameFor(c)));
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

    /**
     *
     * @return  Return a map of all snapshots to space being used
     * The pair for a snapshot has size on disk and true size.
     */
    public Map<String, SnapshotSizeDetails> getSnapshotDetails()
    {
        List<File> snapshots = listSnapshots();
        final Map<String, SnapshotSizeDetails> snapshotSpaceMap = Maps.newHashMapWithExpectedSize(snapshots.size());
        for (File snapshot : snapshots)
        {
            final long sizeOnDisk = FileUtils.folderSize(snapshot);
            final long trueSize = getTrueAllocatedSizeIn(snapshot);
            SnapshotSizeDetails spaceUsed = snapshotSpaceMap.get(snapshot.getName());
            if (spaceUsed == null)
                spaceUsed =  new SnapshotSizeDetails(sizeOnDisk,trueSize);
            else
                spaceUsed = new SnapshotSizeDetails(spaceUsed.sizeOnDiskBytes + sizeOnDisk, spaceUsed.dataSizeBytes + trueSize);
            snapshotSpaceMap.put(snapshot.getName(), spaceUsed);
        }
        return snapshotSpaceMap;
    }

    public List<String> listEphemeralSnapshots()
    {
        final List<String> ephemeralSnapshots = new LinkedList<>();
        for (File snapshot : listSnapshots())
        {
            if (getEphemeralSnapshotMarkerFile(snapshot).exists())
                ephemeralSnapshots.add(snapshot.getName());
        }
        return ephemeralSnapshots;
    }

    private List<File> listSnapshots()
    {
        final List<File> snapshots = new LinkedList<>();
        for (final File dir : dataPaths)
        {
            File snapshotDir = isSecondaryIndexFolder(dir)
                               ? new File(dir.getParent(), SNAPSHOT_SUBDIR)
                               : new File(dir, SNAPSHOT_SUBDIR);
            if (snapshotDir.exists() && snapshotDir.isDirectory())
            {
                final File[] snapshotDirs  = snapshotDir.listFiles();
                if (snapshotDirs != null)
                {
                    for (final File snapshot : snapshotDirs)
                    {
                        if (snapshot.isDirectory())
                            snapshots.add(snapshot);
                    }
                }
            }
        }

        return snapshots;
    }

    public boolean snapshotExists(String snapshotName)
    {
        for (File dir : dataPaths)
        {
            File snapshotDir;
            if (isSecondaryIndexFolder(dir))
            {
                snapshotDir = new File(dir.getParentFile(), join(SNAPSHOT_SUBDIR, snapshotName, dir.getName()));
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

    public static void clearSnapshot(String snapshotName, List<File> snapshotDirectories, RateLimiter snapshotRateLimiter)
    {
        // If snapshotName is empty or null, we will delete the entire snapshot directory
        String tag = snapshotName == null ? "" : snapshotName;
        for (File dir : snapshotDirectories)
        {
            File snapshotDir = new File(dir, join(SNAPSHOT_SUBDIR, tag));
            if (snapshotDir.exists())
            {
                logger.trace("Removing snapshot directory {}", snapshotDir);
                try
                {
                    FileUtils.deleteRecursiveWithThrottle(snapshotDir, snapshotRateLimiter);
                }
                catch (FSWriteError e)
                {
                    if (FBUtilities.isWindows)
                        SnapshotDeletingTask.addFailedSnapshot(snapshotDir);
                    else
                        throw e;
                }
            }
        }
    }

    // The snapshot must exist
    public long snapshotCreationTime(String snapshotName)
    {
        for (File dir : dataPaths)
        {
            File snapshotDir = getSnapshotDirectory(dir, snapshotName);
            if (snapshotDir.exists())
                return snapshotDir.lastModified();
        }
        throw new RuntimeException("Snapshot " + snapshotName + " doesn't exist");
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
                               ? new File(dir.getParent(), SNAPSHOT_SUBDIR)
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

    public long getTrueAllocatedSizeIn(File input)
    {
        if (!input.isDirectory())
            return 0;

        SSTableSizeSummer visitor = new SSTableSizeSummer(input, sstableLister(Directories.OnTxnErr.THROW).listFiles());
        try
        {
            Files.walkFileTree(input.toPath(), visitor);
        }
        catch (IOException e)
        {
            logger.error("Could not calculate the size of {}. {}", input, e.getMessage());
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
            File[] cfDirs = ksDir.listFiles();
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
        return dir.getName().startsWith(SECONDARY_INDEX_NAME_SEPARATOR);
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
        else if (!dir.mkdirs() && !(dir.exists() && dir.isDirectory()))
        {
            throw new FSWriteError(new IOException("Unable to create directory " + dir), dir);
        }
        return dir;
    }

    private static String join(String... s)
    {
        return StringUtils.join(s, File.separator);
    }

    private class SSTableSizeSummer extends DirectorySizeCalculator
    {
        private final Set<String> toSkip;
        SSTableSizeSummer(File path, List<File> files)
        {
            super(path);
            toSkip = files.stream().map(f -> f.getName()).collect(Collectors.toSet());
        }

        @Override
        public boolean isAcceptable(Path path)
        {
            File file = path.toFile();
            Descriptor desc = SSTable.tryDescriptorFromFilename(file);
            return desc != null
                && desc.ksname.equals(metadata.keyspace)
                && desc.cfname.equals(metadata.name)
                && !toSkip.contains(file.getName());
        }
    }

    public static class SnapshotSizeDetails
    {
        public final long sizeOnDiskBytes;
        public final long dataSizeBytes;

        private SnapshotSizeDetails(long sizeOnDiskBytes, long dataSizeBytes)
        {
            this.sizeOnDiskBytes = sizeOnDiskBytes;
            this.dataSizeBytes = dataSizeBytes;
        }

        @Override
        public final int hashCode()
        {
            int hashCode = (int) sizeOnDiskBytes ^ (int) (sizeOnDiskBytes >>> 32);
            return 31 * (hashCode ^ (int) ((int) dataSizeBytes ^ (dataSizeBytes >>> 32)));
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof SnapshotSizeDetails))
                return false;
            SnapshotSizeDetails that = (SnapshotSizeDetails)o;
            return sizeOnDiskBytes == that.sizeOnDiskBytes && dataSizeBytes == that.dataSizeBytes;
        }
    }
}
