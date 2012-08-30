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
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.primitives.Longs;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.compaction.LeveledManifest;
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

    private final String tablename;
    private final String cfname;
    private final File[] sstableDirectories;

    public static Directories create(String tablename, String cfname)
    {
        int idx = cfname.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
        if (idx > 0)
            // secondary index, goes in the same directory than the base cf
            return new Directories(tablename, cfname, cfname.substring(0, idx));
        else
            return new Directories(tablename, cfname, cfname);
    }

    private Directories(String tablename, String cfname, String directoryName)
    {
        this.tablename = tablename;
        this.cfname = cfname;
        this.sstableDirectories = new File[dataFileLocations.length];
        for (int i = 0; i < dataFileLocations.length; ++i)
            sstableDirectories[i] = new File(dataFileLocations[i].location, join(tablename, directoryName));

        if (!StorageService.instance.isClientMode())
        {
            for (File dir : sstableDirectories)
                FileUtils.createDirectory(dir);
        }
    }

    /**
     * Returns SSTable location which is inside given data directory.
     *
     * @param dataDirectory
     * @return SSTable location
     */
    public File getLocationForDisk(File dataDirectory)
    {
        for (File dir : sstableDirectories)
        {
            if (FileUtils.getCanonicalPath(dir).startsWith(FileUtils.getCanonicalPath(dataDirectory)))
                return dir;
        }
        return null;
    }

    public File getDirectoryForNewSSTables(long estimatedSize)
    {
        File path = getLocationWithMaximumAvailableSpace(estimatedSize);

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
            try
            {
                Thread.sleep(10000);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            path = getLocationWithMaximumAvailableSpace(estimatedSize);
        }

        return path;
    }

    /*
     * Loop through all the disks to see which disk has the max free space
     * return the disk with max free space for compactions. If the size of the expected
     * compacted file is greater than the max disk space available return null, we cannot
     * do compaction in this case.
     */
    public File getLocationWithMaximumAvailableSpace(long estimatedSize)
    {
        long maxFreeDisk = 0;
        File maxLocation = null;

        for (File dir : sstableDirectories)
        {
            if (BlacklistedDirectories.isUnwritable(dir))
                continue;

            long usableSpace = dir.getUsableSpace();
            if (maxFreeDisk < usableSpace)
            {
                maxFreeDisk = usableSpace;
                maxLocation = dir;
            }
        }
        // Load factor of 0.9 we do not want to use the entire disk that is too risky.
        maxFreeDisk = (long) (0.9 * maxFreeDisk);
        logger.debug(String.format("expected data files size is %d; largest free partition (%s) has %d bytes free",
                                   estimatedSize, maxLocation, maxFreeDisk));

        return estimatedSize < maxFreeDisk ? maxLocation : null;
    }

    /**
     * Find location which is capable of holding given {@code estimatedSize}.
     * First it looks through for directory with no current task running and
     * the most free space available.
     * If no such directory is available, it just chose the one with the most
     * free space available.
     * If no directory can hold given {@code estimatedSize}, then returns null.
     *
     * @param estimatedSize estimated size you need to find location to fit
     * @return directory capable of given estimated size, or null if none found
     */
    public static DataDirectory getLocationCapableOfSize(long estimatedSize)
    {
        // sort by available disk space
        SortedSet<DataDirectory> directories = ImmutableSortedSet.copyOf(dataFileLocations);

        // if there is disk with sufficient space and no activity running on it, then use it
        for (DataDirectory directory : directories)
        {
            long spaceAvailable = directory.getEstimatedAvailableSpace();
            if (estimatedSize < spaceAvailable && directory.currentTasks.get() == 0)
                return directory;
        }

        // if not, use the one that has largest free space
        if (estimatedSize < directories.first().getEstimatedAvailableSpace())
            return directories.first();
        else
            return null;
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
            return (long)(0.9 * location.getUsableSpace()) - estimatedWorkingSize.get();
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
            final String sstablePrefix = tablename + Component.separator + cfname + Component.separator;
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

    public File getOrCreateLeveledManifest()
    {
        File manifestFile = tryGetLeveledManifest();
        if (manifestFile == null)
            manifestFile = new File(sstableDirectories[0], cfname + LeveledManifest.EXTENSION);
        return manifestFile;
    }

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

    public void clearSnapshot(String snapshotName)
    {
        // If snapshotName is empty or null, we will delete the entire snapshot directory
        String tag = snapshotName == null ? "" : snapshotName;
        for (File dir : sstableDirectories)
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

    private static File getOrCreate(File base, String... subdirs)
    {
        File dir = subdirs == null || subdirs.length == 0 ? base : new File(base, join(subdirs));
        if (dir.exists())
        {
            if (!dir.isDirectory())
                throw new AssertionError(String.format("Invalid directory path %s: path exists but is not a directory", dir));
        }
        else if (!dir.mkdirs())
        {
            throw new FSWriteError(new IOException("Unable to create directory " + dir), dir);
        }
        return dir;
    }

    private static String join(String... s)
    {
        return StringUtils.join(s, File.separator);
    }

    /**
     * To check if sstables needs migration, we look at the System directory.
     * If it does not contain a directory for the schema cfs, we'll attempt a sstable
     * migration.
     *
     * Note that it is mostly harmless to try a migration uselessly, except
     * maybe for some wasted cpu cycles.
     */
    public static boolean sstablesNeedsMigration()
    {
        if (StorageService.instance.isClientMode())
            return false;

        boolean hasSystemKeyspace = false;
        for (DataDirectory dir : dataFileLocations)
        {
            File systemDir = new File(dir.location, Table.SYSTEM_KS);
            hasSystemKeyspace |= (systemDir.exists() && systemDir.isDirectory());
            File statusCFDir = new File(systemDir, SystemTable.SCHEMA_KEYSPACES_CF);
            if (statusCFDir.exists())
                return false;
        }
        if (!hasSystemKeyspace)
            // This is a brand new node.
            return false;

        // Check whether the migration might create too long a filename
        int longestLocation = -1;
        for (DataDirectory loc : dataFileLocations)
            longestLocation = Math.max(longestLocation, FileUtils.getCanonicalPath(loc.location).length());

        // Check that migration won't error out halfway through from too-long paths.  For Windows, we need to check
        // total path length <= 255 (see http://msdn.microsoft.com/en-us/library/aa365247.aspx and discussion on CASSANDRA-2749);
        // elsewhere, we just need to make sure filename is <= 255.
        for (KSMetaData ksm : Schema.instance.getTableDefinitions())
        {
            String ksname = ksm.name;
            for (Map.Entry<String, CFMetaData> entry : ksm.cfMetaData().entrySet())
            {
                String cfname = entry.getKey();

                // max path is roughly (guess-estimate) <location>/ksname/cfname/snapshots/1324314347102-somename/ksname-cfname-tmp-hb-65536-Statistics.db
                if (System.getProperty("os.name").startsWith("Windows")
                    && longestLocation + (ksname.length() + cfname.length()) * 2 + 63 > 255)
                {
                    throw new RuntimeException(String.format("Starting with 1.1, keyspace names and column family " +
                                                             "names must be less than %s characters long. %s/%s doesn't" +
                                                             " respect that restriction. Please rename your " +
                                                             "keyspace/column families to respect that restriction " +
                                                             "before updating.", Schema.NAME_LENGTH, ksname, cfname));
                }

                if (ksm.name.length() + cfname.length() + 28 > 255)
                {
                    throw new RuntimeException("Starting with 1.1, the keyspace name is included in data filenames.  For "
                                               + ksm.name + "/" + cfname + ", this puts you over the largest possible filename of 255 characters");
                }
            }
        }

        return true;
    }

    /**
     * Move sstables from the pre-#2749 layout to their new location/names.
     * This involves:
     *   - moving each sstable to their CF specific directory
     *   - rename the sstable to include the keyspace in the filename
     *
     * Note that this also move leveled manifests, snapshots and backups.
     */
    public static void migrateSSTables()
    {
        logger.info("Upgrade from pre-1.1 version detected: migrating sstables to new directory layout");

        for (DataDirectory dir : dataFileLocations)
        {
            if (!dir.location.exists() || !dir.location.isDirectory())
                continue;

            File[] ksDirs = dir.location.listFiles();
            if (ksDirs != null)
            {
                for (File ksDir : ksDirs)
                {
                    if (!ksDir.isDirectory())
                        continue;

                    File[] files = ksDir.listFiles();
                    if (files != null)
                    {
                        for (File file : files)
                            migrateFile(file, ksDir, null);
                    }

                    migrateSnapshots(ksDir);
                    migrateBackups(ksDir);
                }
            }
        }
    }

    private static void migrateSnapshots(File ksDir)
    {
        File snapshotDir = new File(ksDir, SNAPSHOT_SUBDIR);
        if (!snapshotDir.exists())
            return;

        File[] snapshots = snapshotDir.listFiles();
        if (snapshots != null)
        {
            for (File snapshot : snapshots)
            {
                if (!snapshot.isDirectory())
                    continue;

                File[] files = snapshot.listFiles();
                if (files != null)
                {
                    for (File f : files)
                        migrateFile(f, ksDir, join(SNAPSHOT_SUBDIR, snapshot.getName()));
                }
                if (!snapshot.delete())
                    logger.info("Old snapsot directory {} not deleted by migraation as it is not empty", snapshot);
            }
        }
        if (!snapshotDir.delete())
            logger.info("Old directory {} not deleted by migration as it is not empty", snapshotDir);
    }

    private static void migrateBackups(File ksDir)
    {
        File backupDir = new File(ksDir, BACKUPS_SUBDIR);
        if (!backupDir.exists())
            return;

        File[] files = backupDir.listFiles();
        if (files != null)
        {
            for (File f : files)
                migrateFile(f, ksDir, BACKUPS_SUBDIR);
        }
        if (!backupDir.delete())
            logger.info("Old directory {} not deleted by migration as it is not empty", backupDir);
    }

    private static void migrateFile(File file, File ksDir, String additionalPath)
    {
        if (file.isDirectory())
            return;

        String name = file.getName();
        boolean isManifest = name.endsWith(LeveledManifest.EXTENSION);
        String cfname = isManifest
                      ? name.substring(0, name.length() - LeveledManifest.EXTENSION.length())
                      : name.substring(0, name.indexOf(Component.separator));

        int idx = cfname.indexOf(SECONDARY_INDEX_NAME_SEPARATOR); // idx > 0 => secondary index
        String dirname = idx > 0 ? cfname.substring(0, idx) : cfname;
        File destDir = getOrCreate(ksDir, dirname, additionalPath);

        File destFile = new File(destDir, isManifest ? name : ksDir.getName() + Component.separator + name);
        logger.debug(String.format("[upgrade to 1.1] Moving %s to %s", file, destFile));
        FileUtils.renameWithConfirm(file, destFile);
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
