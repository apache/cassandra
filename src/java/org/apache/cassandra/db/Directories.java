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

import java.io.File;
import java.io.FileFilter;
import java.io.IOError;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

/**
 * Encapsulate handling of paths to the data files.
 *
 * The directory layout is the following:
 *   /<path_to_data_dir>/ks/cf1/ks-cf1-hb-1-Data.db
 *                         /cf2/ks-cf2-hb-1-Data.db
 *                         ...
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

    public static final DataDirectory[] dataFileLocations;
    static
    {
        String[] locations = DatabaseDescriptor.getAllDataFileLocations();
        dataFileLocations = new DataDirectory[locations.length];
        for (int i = 0; i < locations.length; ++i)
            dataFileLocations[i] = new DataDirectory(new File(locations[i]));
    }

    /**
     * Checks whether Cassandra has RWX permissions to the specified directory.
     *
     * @param dir File object of the directory.
     * @param dataDir String representation of the directory's location
     * @return status representing Cassandra's RWX permissions to the supplied folder location.
     */
    public static boolean hasFullPermissions(File dir, String dataDir)
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

    private final String keyspacename;
    private final String cfname;
    private final File[] sstableDirectories;

    public static Directories create(String keyspacename, String cfname)
    {
        int idx = cfname.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
        if (idx > 0)
            // secondary index, goes in the same directory than the base cf
            return new Directories(keyspacename, cfname, cfname.substring(0, idx));
        else
            return new Directories(keyspacename, cfname, cfname);
    }

    private Directories(String keyspacename, String cfname, String directoryName)
    {
        this.keyspacename = keyspacename;
        this.cfname = cfname;
        this.sstableDirectories = new File[dataFileLocations.length];
        for (int i = 0; i < dataFileLocations.length; ++i)
            sstableDirectories[i] = new File(dataFileLocations[i].location, join(keyspacename, directoryName));

        if (!StorageService.instance.isClientMode())
        {
            for (File dir : sstableDirectories)
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
    }

    /**
     * Returns SSTable location which is inside given data directory.
     *
     * @param dataDirectory
     * @return SSTable location
     */
    public File getLocationForDisk(DataDirectory dataDirectory)
    {
        for (File dir : sstableDirectories)
        {
            if (dir.getAbsolutePath().startsWith(dataDirectory.location.getAbsolutePath()))
                return dir;
        }
        return null;
    }

    public File getDirectoryForNewSSTables()
    {
        File path = getWriteableLocationAsFile();

        // Requesting GC has a chance to free space only if we're using mmap and a non SUN jvm
        if (path == null
            && (DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap || DatabaseDescriptor.getIndexAccessMode() == Config.DiskAccessMode.mmap)
            && !FileUtils.isCleanerAvailable())
        {
            logger.info("Forcing GC to free up disk space.  Upgrade to the Oracle JVM to avoid this");
            StorageService.instance.requestGC();
            // retry after GCing has forced unmap of compacted SSTables so they can be deleted
            // Note: GCInspector will do this already, but only sun JVM supports GCInspector so far
            SSTableDeletingTask.rescheduleFailedTasks();
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            path = getWriteableLocationAsFile();
        }

        return path;
    }

    public File getWriteableLocationAsFile()
    {
        return getLocationForDisk(getWriteableLocation());
    }

    /**
     * @return a non-blacklisted directory with the most free space and least current tasks.
     *
     * @throws IOError if all directories are blacklisted.
     */
    public DataDirectory getWriteableLocation()
    {
        List<DataDirectory> candidates = new ArrayList<DataDirectory>();

        // pick directories with enough space and so that resulting sstable dirs aren't blacklisted for writes.
        for (DataDirectory dataDir : dataFileLocations)
        {
            if (BlacklistedDirectories.isUnwritable(getLocationForDisk(dataDir)))
                continue;
            candidates.add(dataDir);
        }

        if (candidates.isEmpty())
            throw new IOError(new IOException("All configured data directories have been blacklisted as unwritable for erroring out"));

        // sort directories by free space, in _descending_ order.
        Collections.sort(candidates);

        // sort directories by load, in _ascending_ order.
        Collections.sort(candidates, new Comparator<DataDirectory>()
        {
            public int compare(DataDirectory a, DataDirectory b)
            {
                return a.currentTasks.get() - b.currentTasks.get();
            }
        });

        return candidates.get(0);
    }


    public static File getSnapshotDirectory(Descriptor desc, String snapshotName)
    {
        return getOrCreate(desc.directory, SNAPSHOT_SUBDIR, snapshotName);
    }

    public static File getBackupsDirectory(Descriptor desc)
    {
        return getOrCreate(desc.directory, BACKUPS_SUBDIR);
    }

    public SSTableLister sstableLister()
    {
        return new SSTableLister();
    }

    public static class DataDirectory implements Comparable<DataDirectory>
    {
        public final File location;
        public final AtomicInteger currentTasks = new AtomicInteger();
        public final AtomicLong estimatedWorkingSize = new AtomicLong();

        public DataDirectory(File location)
        {
            this.location = location;
        }

        /**
         * @return estimated available disk space for bounded directory,
         * excluding the expected size written by tasks in the queue.
         */
        public long getEstimatedAvailableSpace()
        {
            // Load factor of 0.9 we do not want to use the entire disk that is too risky.
            return location.getUsableSpace() - estimatedWorkingSize.get();
        }

        public int compareTo(DataDirectory o)
        {
            // we want to sort by free space in descending order
            return -1 * Longs.compare(getEstimatedAvailableSpace(), o.getEstimatedAvailableSpace());
        }
    }

    public class SSTableLister
    {
        private boolean skipTemporary;
        private boolean includeBackups;
        private boolean onlyBackups;
        private int nbFiles;
        private final Map<Descriptor, Set<Component>> components = new HashMap<Descriptor, Set<Component>>();
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
            List<File> l = new ArrayList<File>(nbFiles);
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

            for (File location : sstableDirectories)
            {
                if (BlacklistedDirectories.isUnreadable(location))
                    continue;

                if (snapshotName != null)
                {
                    new File(location, join(SNAPSHOT_SUBDIR, snapshotName)).listFiles(getFilter());
                    continue;
                }

                if (!onlyBackups)
                    location.listFiles(getFilter());

                if (includeBackups)
                    new File(location, BACKUPS_SUBDIR).listFiles(getFilter());
            }
            filtered = true;
        }

        private FileFilter getFilter()
        {
            // Note: the prefix needs to include cfname + separator to distinguish between a cfs and it's secondary indexes
            final String sstablePrefix = keyspacename + Component.separator + cfname + Component.separator;
            return new FileFilter()
            {
                // This function always return false since accepts adds to the components map
                public boolean accept(File file)
                {
                    // we are only interested in the SSTable files that belong to the specific ColumnFamily
                    if (file.isDirectory() || !file.getName().startsWith(sstablePrefix))
                        return false;

                    Pair<Descriptor, Component> pair = SSTable.tryComponentFromFilename(file.getParentFile(), file.getName());
                    if (pair == null)
                        return false;

                    if (skipTemporary && pair.left.temporary)
                        return false;

                    Set<Component> previous = components.get(pair.left);
                    if (previous == null)
                    {
                        previous = new HashSet<Component>();
                        components.put(pair.left, previous);
                    }
                    previous.add(pair.right);
                    nbFiles++;
                    return false;
                }
            };
        }
    }

    @Deprecated
    public File tryGetLeveledManifest()
    {
        for (File dir : sstableDirectories)
        {
            File manifestFile = new File(dir, cfname + LeveledManifest.EXTENSION);
            if (manifestFile.exists())
            {
                logger.debug("Found manifest at {}", manifestFile);
                return manifestFile;
            }
        }
        logger.debug("No level manifest found");
        return null;
    }

    @Deprecated
    public void snapshotLeveledManifest(String snapshotName)
    {
        File manifest = tryGetLeveledManifest();
        if (manifest != null)
        {
            File snapshotDirectory = getOrCreate(manifest.getParentFile(), SNAPSHOT_SUBDIR, snapshotName);
            File target = new File(snapshotDirectory, manifest.getName());
            FileUtils.createHardLink(manifest, target);
        }
    }

    public boolean snapshotExists(String snapshotName)
    {
        for (File dir : sstableDirectories)
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
                    logger.debug("Removing snapshot directory " + snapshotDir);
                FileUtils.deleteRecursive(snapshotDir);
            }
        }
    }

    // The snapshot must exist
    public long snapshotCreationTime(String snapshotName)
    {
        for (File dir : sstableDirectories)
        {
            File snapshotDir = new File(dir, join(SNAPSHOT_SUBDIR, snapshotName));
            if (snapshotDir.exists())
                return snapshotDir.lastModified();
        }
        throw new RuntimeException("Snapshot " + snapshotName + " doesn't exist");
    }

    // Recursively finds all the sub directories in the KS directory.
    public static List<File> getKSChildDirectories(String ksName)
    {
        List<File> result = new ArrayList<File>();
        for (DataDirectory dataDirectory : dataFileLocations)
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
        List<File> result = new ArrayList<File>();
        for (File dataDirectory : sstableDirectories)
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

    // Hack for tests, don't use otherwise
    static void overrideDataDirectoriesForTest(String loc)
    {
        for (int i = 0; i < dataFileLocations.length; ++i)
            dataFileLocations[i] = new DataDirectory(new File(loc));
    }

    // Hack for tests, don't use otherwise
    static void resetDataDirectoriesAfterTest()
    {
        String[] locations = DatabaseDescriptor.getAllDataFileLocations();
        for (int i = 0; i < locations.length; ++i)
            dataFileLocations[i] = new DataDirectory(new File(locations[i]));
    }
}
