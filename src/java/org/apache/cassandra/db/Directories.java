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

import static com.google.common.collect.Sets.newHashSet;

import java.io.File;
import java.io.FileFilter;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

/**
 * Encapsulate handling of paths to the data files.
 *
 * Since v2.1, the directory layout is the following:
 *   /<path_to_data_dir>/ks/cf1-cfId/ks-cf1-ka-1-Data.db
 *                         /cf2-cfId/ks-cf2-ka-1-Data.db
 *                         ...
 *
 * cfId is an hex encoded CFID.
 *
 * For backward compatibility, Directories uses older directory layout if exists.
 *
 * In addition, more that one 'root' data directory can be specified so that
 * <path_to_data_dir> potentially represents multiple locations.
 * Note that in the case of multiple locations, the manifest for the leveled
 * compaction is only in one of the location.
 *
 * Snapshots (resp. backups) are always created along the sstables thare are
 * snapshoted (resp. backuped) but inside a subdirectory named 'snapshots'
 * (resp. backups) (and snapshots are furter inside a subdirectory of the name
 * of the snapshot).
 *
 * This class abstracts all those details from the rest of the code.
 */
public class Directories
{
    private static final Logger logger = LoggerFactory.getLogger(Directories.class);

    public static final String BACKUPS_SUBDIR = "backups";
    public static final String SNAPSHOT_SUBDIR = "snapshots";
    public static final String SECONDARY_INDEX_NAME_SEPARATOR = ".";

    public static final DataDirectory[] dataDirectories;
    static
    {
        String[] locations = DatabaseDescriptor.getAllDataFileLocations();
        dataDirectories = new DataDirectory[locations.length];
        for (int i = 0; i < locations.length; ++i)
            dataDirectories[i] = new DataDirectory(new File(locations[i]));
    }

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

        private FileAction()
        {
        }

        public static boolean hasPrivilege(File file, FileAction action)
        {
            boolean privilege = false;

            switch (action) {
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

    private final CFMetaData metadata;
    private final File[] dataPaths;

    /**
     * Create Directories of given ColumnFamily.
     * SSTable directories are created under data_directories defined in cassandra.yaml if not exist at this time.
     *
     * @param metadata metadata of ColumnFamily
     */
    public Directories(CFMetaData metadata)
    {
        this.metadata = metadata;

        String cfId = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(metadata.cfId));
        int idx = metadata.cfName.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
        // secondary indicies go in the same directory as the base cf
        String directoryName;
        if (idx >= 0)
        {
            directoryName = metadata.cfName.substring(0, idx) + "-" + cfId + File.separator + metadata.cfName.substring(idx);
        }
        else
        {
             directoryName = metadata.cfName + "-" + cfId;
        }

        this.dataPaths = new File[dataDirectories.length];
        // If upgraded from version less than 2.1, use existing directories
        for (int i = 0; i < dataDirectories.length; ++i)
        {
            // check if old SSTable directory exists
            dataPaths[i] = new File(dataDirectories[i].location, join(metadata.ksName, metadata.cfName));
        }
        boolean olderDirectoryExists = Iterables.any(Arrays.asList(dataPaths), new Predicate<File>()
        {
            public boolean apply(File file)
            {
                return file.exists();
            }
        });
        if (!olderDirectoryExists)
        {
            // use 2.1-style path names
            for (int i = 0; i < dataDirectories.length; ++i)
                dataPaths[i] = new File(dataDirectories[i].location, join(metadata.ksName, directoryName));
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
                FileUtils.handleFSError(e);
            }
        }
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
                if (dir.getAbsolutePath().startsWith(dataDirectory.location.getAbsolutePath()))
                    return dir;
        return null;
    }

    public Descriptor find(String filename)
    {
        for (File dir : dataPaths)
        {
            if (new File(dir, filename).exists())
                return Descriptor.fromFilename(dir, filename).left;
        }
        return null;
    }

    /**
     * Basically the same as calling {@link #getWriteableLocationAsFile(long)} with an unknown size ({@code -1L}),
     * which may return any non-blacklisted directory - even a data directory that has no usable space.
     * Do not use this method in production code.
     *
     * @throws IOError if all directories are blacklisted.
     */
    public File getDirectoryForNewSSTables()
    {
        return getWriteableLocationAsFile(-1L);
    }

    /**
     * Returns a non-blacklisted data directory that _currently_ has {@code writeSize} bytes as usable space.
     *
     * @throws IOError if all directories are blacklisted.
     */
    public File getWriteableLocationAsFile(long writeSize)
    {
        return getLocationForDisk(getWriteableLocation(writeSize));
    }

    /**
     * Returns a non-blacklisted data directory that _currently_ has {@code writeSize} bytes as usable space.
     *
     * @throws IOError if all directories are blacklisted.
     */
    public DataDirectory getWriteableLocation(long writeSize)
    {
        List<DataDirectoryCandidate> candidates = new ArrayList<>();

        long totalAvailable = 0L;

        // pick directories with enough space and so that resulting sstable dirs aren't blacklisted for writes.
        boolean tooBig = false;
        for (DataDirectory dataDir : dataDirectories)
        {
            if (BlacklistedDirectories.isUnwritable(getLocationForDisk(dataDir)))
                continue;
            DataDirectoryCandidate candidate = new DataDirectoryCandidate(dataDir);
            // exclude directory if its total writeSize does not fit to data directory
            if (candidate.availableSpace < writeSize)
            {
                tooBig = true;
                continue;
            }
            candidates.add(candidate);
            totalAvailable += candidate.availableSpace;
        }

        if (candidates.isEmpty())
            if (tooBig)
                return null;
            else
                throw new IOError(new IOException("All configured data directories have been blacklisted as unwritable for erroring out"));

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

        for (DataDirectory dataDir : dataDirectories)
        {
            if (BlacklistedDirectories.isUnwritable(getLocationForDisk(dataDir)))
                  continue;
            DataDirectoryCandidate candidate = new DataDirectoryCandidate(dataDir);
            // exclude directory if its total writeSize does not fit to data directory
            if (candidate.availableSpace < writeSize)
                continue;
            totalAvailable += candidate.availableSpace;
        }
        return totalAvailable > expectedTotalWriteSize;
    }

    public static File getSnapshotDirectory(Descriptor desc, String snapshotName)
    {
        return getSnapshotDirectory(desc.directory, snapshotName);
    }

    public static File getSnapshotDirectory(File location, String snapshotName)
    {
        if (location.getName().startsWith(SECONDARY_INDEX_NAME_SEPARATOR))
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
         return new File(getDirectoryForNewSSTables(), join(SNAPSHOT_SUBDIR, snapshotName, "manifest.json"));
    }

    public static File getBackupsDirectory(Descriptor desc)
    {
        return getBackupsDirectory(desc.directory);
    }

    public static File getBackupsDirectory(File location)
    {
        if (location.getName().startsWith(SECONDARY_INDEX_NAME_SEPARATOR))
        {
            return getOrCreate(location.getParentFile(), BACKUPS_SUBDIR, location.getName());
        }
        else
        {
            return getOrCreate(location, BACKUPS_SUBDIR);
        }
    }

    public SSTableLister sstableLister()
    {
        return new SSTableLister();
    }

    public static class DataDirectory
    {
        public final File location;

        public DataDirectory(File location)
        {
            this.location = location;
        }

        public long getAvailableSpace()
        {
            return location.getUsableSpace();
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

    public class SSTableLister
    {
        private boolean skipTemporary;
        private boolean includeBackups;
        private boolean onlyBackups;
        private int nbFiles;
        private final Map<Descriptor, Set<Component>> components = new HashMap<>();
        private boolean filtered;
        private String snapshotName;

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
                if (BlacklistedDirectories.isUnreadable(location))
                    continue;

                if (snapshotName != null)
                {
                    getSnapshotDirectory(location, snapshotName).listFiles(getFilter());
                    continue;
                }

                if (!onlyBackups)
                    location.listFiles(getFilter());

                if (includeBackups)
                    getBackupsDirectory(location).listFiles(getFilter());
            }
            filtered = true;
        }

        private FileFilter getFilter()
        {
            return new FileFilter()
            {
                // This function always return false since accepts adds to the components map
                public boolean accept(File file)
                {
                    if (file.isDirectory())
                        return false;

                    Pair<Descriptor, Component> pair = SSTable.tryComponentFromFilename(file.getParentFile(), file.getName());
                    if (pair == null)
                        return false;

                    // we are only interested in the SSTable files that belong to the specific ColumnFamily
                    if (!pair.left.ksname.equals(metadata.ksName) || !pair.left.cfname.equals(metadata.cfName))
                        return false;

                    if (skipTemporary && pair.left.type.isTemporary)
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
                }
            };
        }
    }

    /**
     *
     * @return  Return a map of all snapshots to space being used
     * The pair for a snapshot has size on disk and true size.
     */
    public Map<String, Pair<Long, Long>> getSnapshotDetails()
    {
        final Map<String, Pair<Long, Long>> snapshotSpaceMap = new HashMap<>();
        for (final File dir : dataPaths)
        {
            final File snapshotDir = new File(dir,SNAPSHOT_SUBDIR);
            if (snapshotDir.exists() && snapshotDir.isDirectory())
            {
                final File[] snapshots  = snapshotDir.listFiles();
                if (snapshots != null)
                {
                    for (final File snapshot : snapshots)
                    {
                        if (snapshot.isDirectory())
                        {
                            final long sizeOnDisk = FileUtils.folderSize(snapshot);
                            final long trueSize = getTrueAllocatedSizeIn(snapshot);
                            Pair<Long,Long> spaceUsed = snapshotSpaceMap.get(snapshot.getName());
                            if (spaceUsed == null)
                                spaceUsed =  Pair.create(sizeOnDisk,trueSize);
                            else
                                spaceUsed = Pair.create(spaceUsed.left + sizeOnDisk, spaceUsed.right + trueSize);
                            snapshotSpaceMap.put(snapshot.getName(), spaceUsed);
                        }
                    }
                }
            }
        }

        return snapshotSpaceMap;
    }
    public boolean snapshotExists(String snapshotName)
    {
        for (File dir : dataPaths)
        {
            File snapshotDir = new File(dir, join(SNAPSHOT_SUBDIR, snapshotName));
            if (snapshotDir.exists())
                return true;
        }
        return false;
    }

    public static void clearSnapshot(String snapshotName, List<File> snapshotDirectories)
    {
        // If snapshotName is empty or null, we will delete the entire snapshot directory
        String tag = snapshotName == null ? "" : snapshotName;
        for (File dir : snapshotDirectories)
        {
            File snapshotDir = new File(dir, join(SNAPSHOT_SUBDIR, tag));
            if (snapshotDir.exists())
            {
                if (logger.isDebugEnabled())
                    logger.debug("Removing snapshot directory {}", snapshotDir);
                FileUtils.deleteRecursive(snapshotDir);
            }
        }
    }

    // The snapshot must exist
    public long snapshotCreationTime(String snapshotName)
    {
        for (File dir : dataPaths)
        {
            File snapshotDir = new File(dir, join(SNAPSHOT_SUBDIR, snapshotName));
            if (snapshotDir.exists())
                return snapshotDir.lastModified();
        }
        throw new RuntimeException("Snapshot " + snapshotName + " doesn't exist");
    }
    
    public long trueSnapshotsSize()
    {
        long result = 0L;
        for (File dir : dataPaths)
            result += getTrueAllocatedSizeIn(new File(dir, join(SNAPSHOT_SUBDIR)));
        return result;
    }

    public long getTrueAllocatedSizeIn(File input)
    {
        if (!input.isDirectory())
            return 0;
        
        TrueFilesSizeVisitor visitor = new TrueFilesSizeVisitor();
        try
        {
            Files.walkFileTree(input.toPath(), visitor);
        }
        catch (IOException e)
        {
            logger.error("Could not calculate the size of {}. {}", input, e);
        }
    
        return visitor.getAllocatedSize();
    }

    // Recursively finds all the sub directories in the KS directory.
    public static List<File> getKSChildDirectories(String ksName)
    {
        List<File> result = new ArrayList<>();
        for (DataDirectory dataDirectory : dataDirectories)
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

    @VisibleForTesting
    static void overrideDataDirectoriesForTest(String loc)
    {
        for (int i = 0; i < dataDirectories.length; ++i)
            dataDirectories[i] = new DataDirectory(new File(loc));
    }

    @VisibleForTesting
    static void resetDataDirectoriesAfterTest()
    {
        String[] locations = DatabaseDescriptor.getAllDataFileLocations();
        for (int i = 0; i < locations.length; ++i)
            dataDirectories[i] = new DataDirectory(new File(locations[i]));
    }
    
    private class TrueFilesSizeVisitor extends SimpleFileVisitor<Path>
    {
        private final AtomicLong size = new AtomicLong(0);
        private final Set<String> visited = newHashSet(); //count each file only once
        private final Set<String> alive;

        public TrueFilesSizeVisitor()
        {
            super();
            Builder<String> builder = ImmutableSet.builder();
            for (File file: sstableLister().listFiles())
                builder.add(file.getName());
            alive = builder.build();
        }

        private boolean isAcceptable(Path file)
        {
            String fileName = file.toFile().getName();
            Pair<Descriptor, Component> pair = SSTable.tryComponentFromFilename(file.getParent().toFile(), fileName);
            return pair != null
                    && pair.left.ksname.equals(metadata.ksName)
                    && pair.left.cfname.equals(metadata.cfName)
                    && !visited.contains(fileName)
                    && !alive.contains(fileName);
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
        {
            if (isAcceptable(file))
            {
                size.addAndGet(attrs.size());
                visited.add(file.toFile().getName());
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException 
        {
            return FileVisitResult.CONTINUE;
        }
        
        public long getAllocatedSize()
        {
            return size.get();
        }
    }
}
