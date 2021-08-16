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

package org.apache.cassandra.service;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.exceptions.StartupException;

import static org.apache.cassandra.service.FileSystemOwnershipCheck.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileSystemOwnershipCheckTest
{
    private File tempDir;
    private String token;

    @Before
    public void setup() throws IOException
    {
        cleanTempDir();
        tempDir = com.google.common.io.Files.createTempDir();
        token = makeRandomString(10);

        System.setProperty(OWNERSHIP_TOKEN, token);
        System.setProperty(ENABLE_FS_OWNERSHIP_CHECK_PROPERTY, "true");
        System.clearProperty(FS_OWNERSHIP_FILENAME_PROPERTY);
    }

    @After
    public void teardown() throws IOException
    {
        cleanTempDir();
    }

    private void cleanTempDir()
    {
        if (tempDir != null && tempDir.exists())
            delete(tempDir);
    }

    private void delete(File file)
    {
        file.setReadable(true);
        file.setWritable(true);
        File[] files = file.listFiles();
        if (files != null)
        {
            for (File child : files)
            {
                delete(child);
            }
        }
        file.delete();
    }

    // tests for enabling/disabling/configuring the check
    @Test
    public void skipCheckIfDisabled() throws Exception
    {
        // no exceptions thrown from the supplier because the check is skipped
        System.clearProperty(ENABLE_FS_OWNERSHIP_CHECK_PROPERTY);
        checker(() -> { throw new RuntimeException("FAIL"); }).execute();
    }

    @Test
    public void checkEnabledButClusterPropertyIsEmpty()
    {
        System.setProperty(OWNERSHIP_TOKEN, "");
        executeAndFail(checker(tempDir), MISSING_SYSTEM_PROPERTY, OWNERSHIP_TOKEN);
    }

    @Test
    public void checkEnabledButClusterPropertyIsUnset()
    {
        System.clearProperty(OWNERSHIP_TOKEN);
        executeAndFail(checker(tempDir), MISSING_SYSTEM_PROPERTY, OWNERSHIP_TOKEN);
    }

    // tests for presence/absence of files in dirs
    @Test
    public void noRootDirectoryPresent() throws Exception
    {
        executeAndFail(checker("/no/such/location"), NO_OWNERSHIP_FILE, "'/no/such/location'");
    }

    @Test
    public void noDirectoryStructureOrTokenFilePresent() throws Exception
    {
        // The root directory exists, but is completely empty
        executeAndFail(checker(tempDir), NO_OWNERSHIP_FILE, quote(tempDir.getAbsolutePath()));
    }

    @Test
    public void directoryStructureButNoTokenFiles() throws Exception
    {
        File childDir = new File(tempDir, "cassandra/data");
        assertTrue(childDir.mkdirs());
        assertTrue(childDir.exists());
        executeAndFail(checker(childDir), NO_OWNERSHIP_FILE, quote(childDir.getAbsolutePath()));
    }

    @Test
    public void multipleFilesFoundInSameTree() throws Exception
    {
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir, 1, token);
        writeFile(leafDir.getParentFile(), 1, token);
        executeAndFail(checker(leafDir), MULTIPLE_OWNERSHIP_FILES, leafDir);
    }

    @Test
    public void singleValidFileInEachTree() throws Exception
    {
        // Happy path. Each target directory has exactly 1 token file in the
        // dir above it, they all contain the supplied token and the correct
        // count.
        File[] leafDirs = new File[] { mkdirs(tempDir, "d1/data"),
                                       mkdirs(tempDir, "d2/commitlogs"),
                                       mkdirs(tempDir, "d3/hints") };
        for (File dir : leafDirs)
            writeFile(dir.getParentFile(), 3, token);
        checker(leafDirs).execute();
    }

    @Test
    public void multipleDirsSingleTree() throws Exception
    {
        // Happy path. Each target directory has exactly 1 token file in the
        // dir above it (as they all share a single parent). Each contains
        // the supplied token and the correct count (1 in this case).
        File[] leafDirs = new File[] { mkdirs(tempDir, "d1/data"),
                                       mkdirs(tempDir, "d2/commitlogs"),
                                       mkdirs(tempDir, "d3/hints") };
        writeFile(tempDir, 1, token);
        checker(leafDirs).execute();
    }

    @Test
    public void someDirsContainNoFile() throws Exception
    {
        File leafDir1 = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir1, 3, token);
        File leafDir2 = mkdirs(tempDir, "cassandra/commitlogs");
        writeFile(leafDir2, 3, token);
        File leafDir3 = mkdirs(tempDir, "cassandra/hints");

        executeAndFail(checker(leafDir1, leafDir2, leafDir3),
                       NO_OWNERSHIP_FILE,
                       quote(leafDir3.getAbsolutePath()));
    }

    @Test
    public void propsFileUnreadable() throws Exception
    {
        File leafDir = mkdirs(tempDir, "cassandra/data");
        File tokenFile = writeFile(leafDir.getParentFile(), 1, token);
        assertTrue(tokenFile.setReadable(false));
        executeAndFail(checker(leafDir),
                       READ_EXCEPTION,
                       leafDir.getAbsolutePath());
    }

    @Test
    public void propsFileIllegalContent() throws Exception
    {
        File leafDir = mkdirs(tempDir, "cassandra/data");
        File propsFile = new File(leafDir, DEFAULT_FS_OWNERSHIP_FILENAME);
        assertTrue(propsFile.createNewFile());
        try (OutputStream os = Files.newOutputStream(propsFile.toPath()))
        {
            os.write(makeRandomString(40).getBytes());
        }
        assertTrue(propsFile.canRead());
        executeAndFail(checker(leafDir),
                       String.format(INVALID_PROPERTY_VALUE, VERSION),
                       leafDir.getAbsolutePath());
    }

    @Test
    public void propsParentDirUnreadable() throws Exception
    {
        // The props file itself is readable, but its dir is not
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir, 1, token);
        assertTrue(leafDir.setReadable(false));
        checker(leafDir).execute();
    }

    @Test
    public void propsParentDirUntraversable() throws Exception
    {
        // top level dir can't be listed, so no files are found
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.getParentFile(), 1, token);
        assertTrue(tempDir.setExecutable(false));
        executeAndFail(checker(leafDir),
                       NO_OWNERSHIP_FILE,
                       quote(leafDir.getAbsolutePath()));
    }

    @Test
    public void overrideFilename() throws Exception
    {
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.getParentFile(), "other_file", makeProperties(1, 1, token));
        executeAndFail(checker(leafDir), NO_OWNERSHIP_FILE, quote(leafDir.getAbsolutePath()));
        System.setProperty(FS_OWNERSHIP_FILENAME_PROPERTY, "other_file");
        checker(leafDir).execute();
    }

    // check consistency between discovered files
    @Test
    public void differentTokensFoundInTrees() throws Exception
    {
        File file1 = writeFile(mkdirs(tempDir, "d1/data"), 3, token);
        File file2 = writeFile(mkdirs(tempDir, "d2/commitlogs"), 3, token);
        File file3 = writeFile(mkdirs(tempDir, "d3/hints"), 3, "mismatchingtoken");
        String errorSuffix = String.format("['%s', '%s'], ['%s']",
                                           file1.getAbsolutePath(),
                                           file2.getAbsolutePath(),
                                           file3.getAbsolutePath());

        executeAndFail(checker(file1.getParentFile(), file2.getParentFile(), file3.getParentFile()),
                       INCONSISTENT_FILES_FOUND,
                       errorSuffix);
    }

    @Test
    public void differentExpectedCountsFoundInTrees() throws Exception
    {
        File file1 = writeFile(mkdirs(tempDir, "d1/data"), 1, token);
        File file2 = writeFile(mkdirs(tempDir, "d2/commitlogs"), 2, token);
        File file3 = writeFile(mkdirs(tempDir, "d3/hints"), 3, "mismatchingtoken");
        String errorSuffix = String.format("['%s'], ['%s'], ['%s']",
                                           file1.getAbsolutePath(),
                                           file2.getAbsolutePath(),
                                           file3.getAbsolutePath());
        executeAndFail(checker(file1.getParentFile(), file2.getParentFile(), file3.getParentFile()),
                       INCONSISTENT_FILES_FOUND,
                       errorSuffix);
    }

    // tests on property values in discovered files
    @Test
    public void emptyPropertiesFile() throws Exception
    {
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.getParentFile(), DEFAULT_FS_OWNERSHIP_FILENAME, new Properties());
        executeAndFail(checker(leafDir),
                       String.format(INVALID_PROPERTY_VALUE, VERSION),
                       leafDir.getParentFile().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void missingVersionProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VOLUME_COUNT, "1");
        p.setProperty(TOKEN, "foo");
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.getParentFile(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        executeAndFail(checker(leafDir),
                       String.format(INVALID_PROPERTY_VALUE, VERSION),
                       leafDir.getParentFile().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void nonNumericVersionProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VERSION, "abc");
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.getParentFile(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        executeAndFail(checker(leafDir),
                       String.format(INVALID_PROPERTY_VALUE, VERSION),
                       leafDir.getParentFile().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void unsupportedVersionProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VERSION, "99");
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.getParentFile(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        executeAndFail(checker(leafDir),
                       String.format(UNSUPPORTED_VERSION, "99"),
                       leafDir.getParentFile().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void missingVolumeCountProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VERSION, "1");
        p.setProperty(TOKEN, token);
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.getParentFile(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        executeAndFail(checker(leafDir),
                       String.format(INVALID_PROPERTY_VALUE, VOLUME_COUNT),
                       leafDir.getParentFile().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void nonNumericVolumeCountProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VERSION, "1");
        p.setProperty(VOLUME_COUNT, "bar");
        p.setProperty(TOKEN, token);
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.getParentFile(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        executeAndFail(checker(leafDir),
                       String.format(INVALID_PROPERTY_VALUE, VOLUME_COUNT),
                       leafDir.getParentFile().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void missingTokenProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VERSION, "1");
        p.setProperty(VOLUME_COUNT, "1");
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.getParentFile(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        executeAndFail(checker(leafDir),
                       String.format(INVALID_PROPERTY_VALUE, TOKEN),
                       leafDir.getParentFile().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void emptyTokenProp() throws Exception
    {
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.getParentFile(), 1, "");
        executeAndFail(checker(leafDir),
                       String.format(INVALID_PROPERTY_VALUE, TOKEN),
                       leafDir.getParentFile().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void mismatchingTokenProp() throws Exception
    {
        // Ownership token file exists in parent, but content doesn't match property
        File leafDir = mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.getParentFile(), 1, makeRandomString(15));
        executeAndFail(checker(leafDir),
                       MISMATCHING_TOKEN,
                       leafDir.getParentFile().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }


    // Validate volume_count prop values match number of files found
    @Test
    public void expectedVolumeCountMoreThanActual() throws Exception
    {
        // The files on disk indicate that we should expect 2 ownership files,
        // but we only read 1, implying a disk mount is missing
        File[] leafDirs = new File[] { mkdirs(tempDir, "d1/data"),
                                       mkdirs(tempDir, "d2/commitlogs"),
                                       mkdirs(tempDir, "d3/hints") };
        writeFile(tempDir, 2, token);
        executeAndFail(checker(leafDirs), INVALID_FILE_COUNT);
    }

    @Test
    public void expectedVolumeCountLessThanActual() throws Exception
    {
        // The files on disk indicate that we should expect 1 ownership file,
        // but we read 2, implying a extra unexpected disk mount is mounted
        File leafDir1 = mkdirs(tempDir, "d1/data");
        writeFile(leafDir1, 1, token);
        File leafDir2 = mkdirs(tempDir, "d2/commitlogs");
        writeFile(leafDir2, 1, token);
        executeAndFail(checker(leafDir1, leafDir2), INVALID_FILE_COUNT);
    }

    private static void executeAndFail(FileSystemOwnershipCheck checker, String messageTemplate, Object...messageArgs)
    {
        try
        {
            checker.execute();
            fail("Expected an exception but none thrown");
        } catch (StartupException e) {
            String expected = ERROR_PREFIX + String.format(messageTemplate, messageArgs);
            assertEquals(expected, e.getMessage());
        }
    }

    private static Properties makeProperties(int version, int volumeCount, String token)
    {
        Properties props = new Properties();
        props.setProperty(VERSION, Integer.toString(version));
        props.setProperty(VOLUME_COUNT, Integer.toString(volumeCount));
        props.setProperty(TOKEN, token);
        return props;
    }

    private static File writeFile(File dir, int volumeCount, String token) throws IOException
    {
        return writeFile(dir, DEFAULT_FS_OWNERSHIP_FILENAME, 1, volumeCount, token);
    }

    private static File writeFile(File dir, final String filename, int version, int volumeCount, String token)
    throws IOException
    {
        return writeFile(dir, filename, makeProperties(version, volumeCount, token));
    }

    private static File writeFile(File dir, String filename, Properties props) throws IOException
    {
        File tokenFile = new File(dir, filename);
        assertTrue(tokenFile.createNewFile());
        try (OutputStream os = Files.newOutputStream(tokenFile.toPath()))
        {
            props.store(os, "Test properties");
        }
        assertTrue(tokenFile.canRead());
        return tokenFile;
    }

    private static File mkdirs(File parent, String path)
    {
        File childDir = new File(parent, path);
        assertTrue(childDir.mkdirs());
        assertTrue(childDir.exists());
        return childDir;
    }

    private static FileSystemOwnershipCheck checker(Supplier<Iterable<String>> dirs)
    {
        return new FileSystemOwnershipCheck(dirs);
    }

    private static FileSystemOwnershipCheck checker(File...dirs)
    {
        return checker(() -> Arrays.stream(dirs).map(File::getAbsolutePath).collect(Collectors.toList()));
    }

    private static FileSystemOwnershipCheck checker(String...dirs)
    {
        return checker(() -> Arrays.asList(dirs));
    }

    public static String makeRandomString(int length)
    {
        Random random = new Random();
        char[] chars = new char[length];
        for (int i = 0; i < length; ++i)
            chars[i] = (char) ('a' + random.nextInt('z' - 'a' + 1));
        return new String(chars);
    }

    private String quote(String toQuote)
    {
        return String.format("'%s'", toQuote);
    }
}