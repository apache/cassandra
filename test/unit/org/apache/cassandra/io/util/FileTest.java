/**
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
 */
package org.apache.cassandra.io.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import org.junit.Assert;
import org.junit.Test;

import org.psjava.util.Triple;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_IO_TMPDIR;

public class FileTest
{
    private static final java.io.File dir;
    static
    {
        java.io.File parent = new java.io.File(JAVA_IO_TMPDIR.getString());
        String dirName = Long.toHexString(ThreadLocalRandom.current().nextLong());
        while (new java.io.File(parent, dirName).exists())
            dirName = Long.toHexString(ThreadLocalRandom.current().nextLong());
        dir = new java.io.File(parent, dirName);
        dir.mkdirs();
        new File(dir).deleteRecursiveOnExit();
    }

    @Test
    public void testEquivalence() throws IOException
    {
        java.io.File notExists = new java.io.File(dir, "notExists");
        java.io.File regular = new java.io.File(dir, "regular");
        regular.createNewFile();
        java.io.File regularLink = new java.io.File(dir, "regularLink");
        Files.createSymbolicLink(regularLink.toPath(), regular.toPath());
        java.io.File emptySubdir = new java.io.File(dir, "empty");
        java.io.File emptySubdirLink = new java.io.File(dir, "emptyLink");
        emptySubdir.mkdir();
        Files.createSymbolicLink(emptySubdirLink.toPath(), emptySubdir.toPath());
        java.io.File nonEmptySubdir = new java.io.File(dir, "nonEmpty");
        java.io.File nonEmptySubdirLink = new java.io.File(dir, "nonEmptyLink");
        nonEmptySubdir.mkdir();
        Files.createSymbolicLink(nonEmptySubdirLink.toPath(), nonEmptySubdir.toPath());
        new java.io.File(nonEmptySubdir, "something").createNewFile();

        testEquivalence("");

        List<Runnable> setup = ImmutableList.of(
            () -> {},
            () -> dir.setWritable(false),
            () -> dir.setReadable(false),
            () -> dir.setWritable(true)
        );

        for (Runnable run : setup)
        {
            run.run();
            testEquivalence(notExists.getPath());
            testEquivalence(nonAbsolute(notExists));
            testEquivalence(regular.getPath());
            testEquivalence(nonAbsolute(regular));
            testEquivalence(regularLink.getPath());
            testEquivalence(nonAbsolute(regularLink));
            testEquivalence(emptySubdir.getPath());
            testEquivalence(nonAbsolute(emptySubdir));
            testEquivalence(emptySubdirLink.getPath());
            testEquivalence(nonAbsolute(emptySubdirLink));
            testEquivalence(nonEmptySubdir.getPath());
            testEquivalence(nonAbsolute(nonEmptySubdir));
            testEquivalence(nonEmptySubdirLink.getPath());
            testEquivalence(nonAbsolute(nonEmptySubdirLink));
        }

        emptySubdirLink.delete();
        regularLink.delete();
        regular.delete();
        emptySubdir.delete();
    }

    private static String nonAbsolute(java.io.File file)
    {
        return file.getParent() + File.pathSeparator() + ".." + File.pathSeparator() + file.getParentFile().getName() + File.pathSeparator() + file.getName();
    }

    private void testEquivalence(String path) throws IOException
    {
        java.io.File file = new java.io.File(path);
        if (file.exists()) testExists(path);
        else testNotExists(path);
    }

    private void testBasic(String path) throws IOException
    {
        // TODO: confirm - it seems that accuracy of lastModified may differ between APIs on Linux??
        testEquivalence(path, f -> f.lastModified() / 1000, f -> f.lastModified() / 1000);
        testEquivalence(path, java.io.File::length, File::length);
        testEquivalence(path, java.io.File::canExecute, File::isExecutable);
        testEquivalence(path, java.io.File::canRead, File::isReadable);
        testEquivalence(path, java.io.File::canWrite, File::isWritable);
        testEquivalence(path, java.io.File::exists, File::exists);
        testEquivalence(path, java.io.File::isAbsolute, File::isAbsolute);
        testEquivalence(path, java.io.File::isDirectory, File::isDirectory);
        testEquivalence(path, java.io.File::isFile, File::isFile);
        testEquivalence(path, java.io.File::getPath, File::path);
        testEquivalence(path, java.io.File::getAbsolutePath, File::absolutePath);
        testEquivalence(path, java.io.File::getCanonicalPath, File::canonicalPath);
        testEquivalence(path, java.io.File::getParent, File::parentPath);
        testEquivalence(path, java.io.File::toPath, File::toPath);
        testEquivalence(path, java.io.File::list, File::tryListNames);
        testEquivalence(path, java.io.File::listFiles, File::tryList);
        java.io.File file = new java.io.File(path);
        if (file.getParentFile() != null) testBasic(file.getParent());
        if (!file.equals(file.getAbsoluteFile())) testBasic(file.getAbsolutePath());
        if (!file.equals(file.getCanonicalFile())) testBasic(file.getCanonicalPath());
    }

    private void testPermissionsEquivalence(String path)
    {
        ImmutableList<Triple<BiFunction<java.io.File, Boolean, Boolean>, BiFunction<File, Boolean, Boolean>, Function<java.io.File, Boolean>>> tests = ImmutableList.of(
            Triple.create(java.io.File::setReadable, File::trySetReadable, java.io.File::canRead),
            Triple.create(java.io.File::setWritable, File::trySetWritable, java.io.File::canWrite),
            Triple.create(java.io.File::setExecutable, File::trySetExecutable, java.io.File::canExecute)
        );
        for (Triple<BiFunction<java.io.File, Boolean, Boolean>, BiFunction<File, Boolean, Boolean>, Function<java.io.File, Boolean>> test : tests)
        {
            java.io.File file = new java.io.File(path);
            boolean cur = test.v3.apply(file);
            boolean canRead = file.canRead();
            boolean canWrite = file.canWrite();
            boolean canExecute = file.canExecute();
            testEquivalence(path, f -> test.v1.apply(f, !cur), f -> test.v2.apply(f, !cur), (f, success) -> {
                testEquivalence(path, java.io.File::canExecute, File::isExecutable);
                testEquivalence(path, java.io.File::canRead, File::isReadable);
                testEquivalence(path, java.io.File::canWrite, File::isWritable);
                Assert.assertEquals(success != cur, test.v3.apply(file));
                test.v1.apply(f, cur);
            });
            Assert.assertEquals(canRead, file.canRead());
            Assert.assertEquals(canWrite, file.canWrite());
            Assert.assertEquals(canExecute, file.canExecute());
        }
    }

    private void testCreation(String path, IOConsumer<java.io.File> afterEach)
    {
        testEquivalence(path, java.io.File::createNewFile, File::createFileIfNotExists, afterEach);
        testEquivalence(path, java.io.File::mkdir, File::tryCreateDirectory, afterEach);
        testEquivalence(path, java.io.File::mkdirs, File::tryCreateDirectories, afterEach);
    }

    private void testExists(String path) throws IOException
    {
        testBasic(path);
        testPermissionsEquivalence(path);
        testCreation(path, ignore -> {});
        testEquivalence(path, java.io.File::delete, File::tryDelete, (f, s) -> {if (s) f.createNewFile(); });
        testTryVsConfirm(path, java.io.File::delete, File::delete, (f, s) -> {if (s) f.createNewFile(); });
    }

    private void testNotExists(String path) throws IOException
    {
        testBasic(path);
        testPermissionsEquivalence(path);
        testCreation(path, java.io.File::delete);
        testEquivalence(path, java.io.File::delete, File::tryDelete);
        testTryVsConfirm(path, java.io.File::delete, File::delete);
    }

    interface IOFn<I, O> { O apply(I in) throws IOException; }
    interface IOConsumer<I1> { void accept(I1 i1) throws IOException; }
    interface IOBiConsumer<I1, I2> { void accept(I1 i1, I2 i2) throws IOException; }

    private <T> void testEquivalence(String path, IOFn<java.io.File, T> canonical, IOFn<File, T> test)
    {
        testEquivalence(path, canonical, test, ignore -> {});
    }

    private <T> void testEquivalence(String path, IOFn<java.io.File, T> canonical, IOFn<File, T> test, IOConsumer<java.io.File> afterEach)
    {
        testEquivalence(path, canonical, test, (f, ignore) -> afterEach.accept(f));
    }

    private <T> void testEquivalence(String path, IOFn<java.io.File, T> canonical, IOFn<File, T> test, IOBiConsumer<java.io.File, Boolean> afterEach)
    {
        java.io.File file = new java.io.File(path);
        Object expect;
        try
        {
            expect = canonical.apply(file);
        }
        catch (Throwable e)
        {
            expect = new Failed(e);
        }
        try { afterEach.accept(file, !(expect instanceof Failed) && !Boolean.FALSE.equals(expect)); } catch (IOException e) { throw new AssertionError(e); }
        Object actual;
        try
        {
            actual = test.apply(new File(path));
        }
        catch (Throwable e)
        {
            actual = new Failed(e);
        }
        try { afterEach.accept(file, !(actual instanceof Failed) && !Boolean.FALSE.equals(actual)); } catch (IOException e) { throw new AssertionError(e); }
        if (expect instanceof String[] && actual instanceof String[]) Assert.assertArrayEquals((String[])expect, (String[])actual);
        else if (expect instanceof java.io.File[] && actual instanceof File[]) assertArrayEquals((java.io.File[]) expect, (File[]) actual);
        else Assert.assertEquals(path + "," + canonical.toString(), expect, actual);
    }

    private void testTryVsConfirm(String path, Predicate<java.io.File> canonical, IOConsumer<File> test)
    {
        testTryVsConfirm(path, canonical, test, (f, s) -> {});
    }
    private void testTryVsConfirm(String path, Predicate<java.io.File> canonical, IOConsumer<File> test, IOConsumer<java.io.File> afterEach)
    {
        testTryVsConfirm(path, canonical, test, (f, ignore) -> afterEach.accept(f));
    }
    private void testTryVsConfirm(String path, Predicate<java.io.File> canonical, IOConsumer<File> test, IOBiConsumer<java.io.File, Boolean> afterEach)
    {
        java.io.File file = new java.io.File(path);
        boolean expect = canonical.test(file);
        try { afterEach.accept(file, expect); } catch (IOException e) { throw new AssertionError(e); }
        boolean actual;
        try
        {
            test.accept(new File(path));
            actual = true;
        }
        catch (Throwable e)
        {
            actual = false;
        }
        try { afterEach.accept(file, actual); } catch (IOException e) { throw new AssertionError(e); }
        Assert.assertEquals(path + "," + canonical.toString(), expect, actual);
    }

    private static void assertArrayEquals(java.io.File[] expect, File[] actual)
    {
        Assert.assertEquals(expect.length, actual.length);
        for (int i = 0 ; i < expect.length ; ++i)
            Assert.assertEquals(expect[i].getPath(), actual[i].path());
    }

    private static class Failed
    {
        final Throwable with;

        private Failed(Throwable with)
        {
            this.with = with;
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj instanceof Failed;
        }

        @Override
        public String toString()
        {
            StringWriter sw = new StringWriter();
            with.printStackTrace(new PrintWriter(sw));
            return sw.toString();
        }
    }

    @Test
    public void testDeletes() throws IOException
    {
        File subdir = new File(dir, "deletes");
        File file = new File(dir, "f");
        subdir.tryCreateDirectory();
        Assert.assertTrue(new File(subdir, "subsubdir").tryCreateDirectory());
        subdir.deleteRecursive();
        Assert.assertFalse(subdir.exists());

        subdir.tryCreateDirectory();
        file.createFileIfNotExists();
        Assert.assertTrue(new File(subdir, "subsubdir").tryCreateDirectory());
        long start = System.nanoTime();
        RateLimiter rateLimiter = RateLimiter.create(2);
        subdir.deleteRecursive(rateLimiter);
        file.delete(rateLimiter);
        long end = System.nanoTime();
        Assert.assertTrue("" + NANOSECONDS.toMillis(end - start), SECONDS.toNanos(1) <= end - start);
        Assert.assertFalse(subdir.exists());
        Assert.assertFalse(file.exists());
    }

    @Test
    public void testDeleteWithAccumulate()
    {
        File file = new File(dir, "testdelete");
        Assert.assertTrue(file.tryCreateDirectory());

        Throwable accumulate = null;
        accumulate = file.delete(accumulate);
        Assert.assertNull(accumulate);
        accumulate = file.delete(accumulate);
        Assert.assertNotNull(accumulate);
        Assert.assertTrue(accumulate instanceof UncheckedIOException);
        Assert.assertTrue(accumulate.getCause() instanceof NoSuchFileException);
    }

    @Test
    public void testForEachRecursive() throws IOException
    {
        File subdir = new File(dir, "forEachRecursive");
        Assert.assertTrue(subdir.tryCreateDirectory());
        Assert.assertTrue(new File(subdir, "subsubdir").tryCreateDirectory());
        File file = new File(subdir, "f");
        Assert.assertTrue(file.createFileIfNotExists());
        Assert.assertTrue(subdir.exists());
        Assert.assertTrue(file.exists());
        
        subdir.forEachRecursive(f -> f.toPath());
    }

    @Test
    public void testList()
    {
        File subdir = new File(dir, "testList");
        Assert.assertTrue(subdir.tryCreateDirectory());
        Assert.assertTrue(new File(subdir, "subsubdir").tryCreateDirectory());
        File file = new File(subdir, "f");
        Assert.assertTrue(file.createFileIfNotExists());
        Assert.assertTrue(subdir.exists());
        Assert.assertTrue(file.exists());

        Function<Stream<File>, Stream<File>> toFiles = Function.identity();
        String[] result = PathUtils.list(subdir.path, stream -> toFiles.apply(stream.map(File::new)).map(File::name), String[]::new);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.length == 2);
    }

    @Test
    public void testAncestry()
    {
        Assert.assertTrue(new File("somewhere/../").isAncestorOf(new File("somewhere")));
        Assert.assertTrue(new File("../").isAncestorOf(new File("")));
    }
}
