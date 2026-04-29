import org.gradle.api.Project

/**
 * Utility methods for configuring Cassandra test tasks.
 */
class CassandraTestSuite {

    /**
     * Scans sourceDir for files matching *Test.java and returns a list of
     * fully-qualified class-name patterns suitable for Test.include().
     * E.g. "org/apache/cassandra/db/SomeTest.java" → "org/apache/cassandra/db/SomeTest.class"
     */
    static List<String> scanTestClassPatterns(Project project, String sourceDir) {
        def dir = project.file(sourceDir)
        if (!dir.exists()) return []
        def patterns = []
        project.fileTree(dir: dir, include: '**/*Test.java').each { File f ->
            def rel = dir.toPath().relativize(f.toPath()).toString()
            patterns << rel.replaceAll(/\.java$/, '.class')
        }
        return patterns
    }

    /**
     * Base JVM args common to all test suites (mirrors testmacrohelper in build.xml).
     * Does NOT include JAMM javaagent (resolved lazily at execution time).
     */
    static List<String> baseTestJvmArgs(Project project, int jdkVersion) {
        // Stack size: -Xss256k on x86, -Xss384k otherwise
        def arch = System.getProperty('os.arch', '')
        def xss = (arch == 'amd64' || arch == 'x86_64') ? '-Xss256k' : '-Xss384k'

        // Use absolute path for storage-config to match ant behavior
        def testConf = "${project.projectDir}/test/conf"

        def args = [
            "-Dstorage-config=${testConf}",
            '-Djava.awt.headless=true',
            '-ea',
            "-Djava.io.tmpdir=${project.layout.buildDirectory.get()}/tmp",
            '-Dcassandra.debugrefcount=true',
            '-Xms512M',
            xss,
            '-XX:SoftRefLRUPolicyMSPerMB=0',
            '-XX:ActiveProcessorCount=2',
            "-XX:HeapDumpPath=${project.layout.buildDirectory.get()}/test",
            '-Dcassandra.test.accord.allow_test_modes=true',
            '-Dcassandra.test.driver.connection_timeout_ms=10000',
            '-Dcassandra.test.driver.read_timeout_ms=24000',
            '-Dcassandra.memtable_row_overhead_computation_step=100',
            '-Dcassandra.test.use_prepared=true',
            '-Dcassandra.test.sstableformatdevelopment=true',
            '-Djava.security.egd=file:/dev/urandom',
            "-Dcassandra.testtag=_jdk${jdkVersion}",
            '-Dcassandra.keepBriefBrief=true',
            '-Dcassandra.strict.runtime.checks=true',
            '-Dcassandra.reads.thresholds.coordinator.defensive_checks_enabled=true',
            '-Dcassandra.test.flush_local_schema_changes=false',
            '-Dcassandra.test.messagingService.nonGracefulShutdown=true',
            '-Dcassandra.use_nix_recursive_delete=true',
            '-Dio.netty.allocator.useCacheForAllThreads=true',
            '-Dio.netty.allocator.maxOrder=11',
            '-DQT_SHRINKS=0',
        ]

        // JDK-specific runtime + test args
        args.addAll(JdkJvmArgs.jvmArgs(jdkVersion))
        args.addAll(JdkJvmArgs.testArgs(jdkVersion))

        return args
    }

    /**
     * Standard per-suite JVM args (ring_delay, skip_sync, legacy sstable roots).
     */
    static List<String> standardSuiteArgs(Project project) {
        def testData = "${project.projectDir}/test/data"
        return [
            '-Dcassandra.ring_delay_ms=1000',
            '-Dcassandra.tolerate_sstable_size=true',
            '-Dcassandra.skip_sync=true',
            "-Dlegacy-sstable-root=${testData}/legacy-sstables",
            "-Dinvalid-legacy-sstable-root=${testData}/invalid-legacy-sstables",
        ]
    }

    /**
     * Simulator-specific JVM args.
     */
    static List<String> simulatorSuiteArgs(Project project) {
        def simLibDir = "${project.layout.buildDirectory.get()}/test/lib/jars"
        return [
            '-Djdk.attach.allowAttachSelf=true',
            '-Dlogback.configurationFile=test/conf/logback-simulator.xml',
            '-Dcassandra.ring_delay_ms=10000',
            '-Dcassandra.tolerate_sstable_size=true',
            '-Dcassandra.skip_sync=true',
            '-Dcassandra.debugrefcount=false',
            '-Dcassandra.keepBriefBrief=false',
            '-Dcassandra.test.simulator.determinismcheck=strict',
            '-Dcassandra.test.simulator.print_asm=none',
            "-javaagent:${simLibDir}/simulator-asm.jar",
            "-Xbootclasspath/a:${simLibDir}/simulator-bootstrap.jar",
            '-XX:ActiveProcessorCount=4',
            '-XX:-TieredCompilation',
            '-XX:-BackgroundCompilation',
            '-XX:CICompilerCount=1',
            '-XX:Tier4CompileThreshold=1000',
            '-XX:ReservedCodeCacheSize=256M',
            '-XX:MaxDirectMemorySize=8G',
        ]
    }

    /**
     * Resolve JAMM jar from testRuntimeClasspath (must be called at execution time).
     */
    static File resolveJammJar(Project project) {
        try {
            def testRuntime = project.configurations.findByName('testRuntimeClasspath')
            if (testRuntime) {
                return testRuntime.resolvedConfiguration.resolvedArtifacts.find {
                    it.moduleVersion.id.group == 'com.github.jbellis' && it.moduleVersion.id.name == 'jamm'
                }?.file
            }
        } catch (Exception ignored) {}
        return null
    }

    /**
     * Configure a Test task with standard Cassandra test settings.
     *
     * @param task       the Test task
     * @param config     a map with keys:
     *   - project:      Project
     *   - jdkVersion:   int
     *   - antProps:     Map
     *   - sourceDir:    String (e.g. 'test/unit')
     *   - timeout:      long (ms)
     *   - maxHeap:      String (e.g. '1g')
     *   - isSimulator:  boolean (default false)
     */
    static void configure(org.gradle.api.tasks.testing.Test task, Map config) {
        Project project = config.project
        int jdkVersion = config.jdkVersion as int
        String sourceDir = config.sourceDir
        long timeout = config.timeout as long
        String maxHeap = config.maxHeap ?: '1g'
        boolean isSimulator = config.isSimulator ?: false

        task.useJUnit()
        task.maxHeapSize = maxHeap
        task.testClassesDirs = project.sourceSets.test.output.classesDirs

        // Include only tests from the specified source directory
        def patterns = scanTestClassPatterns(project, sourceDir)
        if (patterns) {
            task.include(patterns)
        }

        // Timeout
        task.timeout.set(java.time.Duration.ofMillis(timeout))

        // JVM args (without JAMM — added in doFirst)
        def jvmArgs = baseTestJvmArgs(project, jdkVersion)
        if (isSimulator) {
            jvmArgs.addAll(simulatorSuiteArgs(project))
        } else {
            jvmArgs.addAll(standardSuiteArgs(project))
        }
        // Set suitename for logback-test.xml log file path resolution
        jvmArgs.add("-Dsuitename=${task.name}")
        task.jvmArgs(jvmArgs)

        // Working directory
        task.workingDir = project.projectDir

        // Forking
        task.forkEvery = isSimulator ? 1 : 0  // perTest for simulator

        // Classpath
        task.classpath = project.sourceSets.test.runtimeClasspath

        // Ensure build directories exist and resolve JAMM javaagent at execution time
        task.doFirst {
            project.file("${project.layout.buildDirectory.get()}/tmp").mkdirs()
            project.file("${project.layout.buildDirectory.get()}/test/cassandra").mkdirs()
            project.file("${project.layout.buildDirectory.get()}/test/output").mkdirs()

            // Add JAMM javaagent (resolved lazily to avoid configuration-time resolution)
            def jammJar = resolveJammJar(project)
            if (jammJar) {
                task.jvmArgs("-javaagent:${jammJar}")
            }
        }

        // Variant support
        def variant = project.findProperty('variant')
        if (variant) {
            def variants = TestVariantConfig.all()
            def variantConfig = variants[variant]
            if (variantConfig) {
                task.doFirst {
                    // Concatenate YAML overlays
                    def overlayFile = project.file("${project.layout.buildDirectory.get()}/test/cassandra.${variant}.yaml")
                    overlayFile.parentFile.mkdirs()
                    overlayFile.text = project.file('test/conf/cassandra.yaml').text
                    variantConfig.overlayYamls.each { yaml ->
                        overlayFile.append('\n')
                        overlayFile.append(project.file(yaml).text)
                    }
                    task.systemProperty('cassandra.config', "file:///${overlayFile.absolutePath}")

                    // Merge variant system properties
                    variantConfig.systemProperties.each { k, v ->
                        task.systemProperty(k, v)
                    }
                }
            } else {
                throw new org.gradle.api.GradleException("Unknown variant '${variant}'. Known: ${variants.keySet()}")
            }
        }

        // Test output
        task.reports.junitXml.required = true
        task.reports.junitXml.outputLocation = project.layout.buildDirectory.dir("test/output/${task.name}")
        task.reports.html.required = true
        task.reports.html.outputLocation = project.layout.buildDirectory.dir("test/reports/${task.name}")
    }
}
