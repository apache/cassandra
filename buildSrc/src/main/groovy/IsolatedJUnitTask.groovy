import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.FileCollection
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.Nested
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.options.Option
import org.gradle.jvm.toolchain.JavaLauncher

/**
 * Runs JUnit 4 tests in a clean JVM via the JUnit Platform ConsoleLauncher,
 * bypassing Gradle's Test task infrastructure (GradleWorkerMain, injected
 * system properties, worker threads).
 *
 * Created for the simulator, which requires exact control over all threads
 * in the JVM.  Gradle's worker threads cause the simulator to deadlock.
 *
 * Forks one JVM per test class.  Supports {@code --tests} for class and
 * method filtering.  Writes JUnit XML reports to the standard Gradle location.
 */
class IsolatedJUnitTask extends DefaultTask {

    @Classpath
    FileCollection testClasspath

    @Classpath
    FileCollection launcherClasspath

    @Input
    List<String> testJvmArgs = []

    @Input
    List<String> testClasses = []

    @Input @Optional
    String maxHeapSize

    @Input @Optional
    String minHeapSize

    @Internal
    File workDir

    @Nested
    final Property<JavaLauncher> javaLauncher = project.objects.property(JavaLauncher)

    // --tests filter: "FooTest", "FooTest.barMethod", "*.FooTest", or FQCN variants
    private List<String> testFilters = []

    @Option(option = "tests", description = "Test class or method filter")
    void setTestFilters(List<String> filters) {
        this.testFilters = filters
    }

    @Input @Optional
    List<String> getTestFilters() {
        return this.testFilters
    }

    /** Called before each fork to transform jvmArgs (e.g. prepend JAMM agent). */
    @Internal
    Closure<List<String>> jvmArgsTransformer

    @TaskAction
    void runTests() {
        def xmlOutputDir = new File("${project.layout.buildDirectory.get()}/test/output/${name}")
        xmlOutputDir.mkdirs()

        def selections = resolveSelections()
        if (selections.isEmpty()) {
            throw new GradleException("No test classes found for task '${name}'")
        }

        logger.lifecycle("Running ${selections.size()} test class(es) for ${name}")

        def failures = []
        def totalTests = 0
        def totalFailures = 0

        selections.each { Selection sel ->
            def className = sel.className
            logger.lifecycle("  Forking JVM for: ${className}${sel.methodName ? '#' + sel.methodName : ''}")

            def tempReportDir = new File(project.layout.buildDirectory.get().asFile,
                    "tmp/isolated-junit/${name}/${className}")
            tempReportDir.mkdirs()

            def clArgs = [
                '--include-engine', 'junit-vintage',
                '--disable-banner',
                '--details', 'flat',
                '--reports-dir', tempReportDir.absolutePath,
            ]
            if (sel.methodName) {
                clArgs += ['--select-method', "${className}#${sel.methodName}"]
            } else {
                clArgs += ['--select-class', className]
            }

            def resolvedJvmArgs = new ArrayList(testJvmArgs)
            if (jvmArgsTransformer) {
                resolvedJvmArgs = jvmArgsTransformer.call(resolvedJvmArgs)
            }

            def execResult = project.javaexec { spec ->
                spec.executable = javaLauncher.get().executablePath.asFile.absolutePath
                spec.classpath = testClasspath + launcherClasspath
                spec.mainClass.set('org.junit.platform.console.ConsoleLauncher')
                spec.args = clArgs
                spec.jvmArgs = resolvedJvmArgs
                if (maxHeapSize) spec.maxHeapSize = maxHeapSize
                if (minHeapSize) spec.minHeapSize = minHeapSize
                if (workDir) spec.workingDir = workDir
                spec.ignoreExitValue = true
            }

            // Rename XML to TEST-{className}.xml to match Gradle convention
            def sourceXml = new File(tempReportDir, 'TEST-junit-vintage.xml')
            if (sourceXml.exists()) {
                def targetXml = new File(xmlOutputDir, "TEST-${className}.xml")
                targetXml.text = sourceXml.text

                def xmlText = targetXml.text
                def testCount = (xmlText =~ /tests="(\d+)"/)[0]?[1] ?: '0'
                def failCount = (xmlText =~ /(?:failures|errors)="([1-9]\d*)"/).collect { it[1] }
                totalTests += testCount as int
                def classFailures = failCount.collect { it as int }.sum() ?: 0
                totalFailures += classFailures
                if (classFailures > 0) {
                    failures << className
                }
            }

            // Exit codes: 0=success, 1=test failure, 2=no tests found
            if (execResult.exitValue == 2) {
                throw new GradleException("No tests found for class '${className}' in task '${name}'")
            }
            if (execResult.exitValue != 0 && execResult.exitValue != 1) {
                failures << "${className} (JVM crashed with exit code ${execResult.exitValue})"
            }
        }

        logger.lifecycle("${name}: ${totalTests} tests, ${totalFailures} failures across ${selections.size()} class(es)")

        if (failures) {
            throw new GradleException(
                "There were failing tests in ${name}. Failed classes: ${failures.join(', ')}\n" +
                "  Reports: ${xmlOutputDir.absolutePath}")
        }
    }

    /** Resolve --tests filters against configured test classes. */
    List<Selection> resolveSelections() {
        if (testFilters.isEmpty()) {
            // No --tests: run all configured test classes
            return testClasses.collect { new Selection(className: it) }
        }

        def selections = []
        testFilters.each { filter ->
            selections.addAll(resolveFilter(filter))
        }
        return selections
    }

    List<Selection> resolveFilter(String filter) {
        // Heuristic: if the last dot-separated segment starts with lowercase and
        // what's before it looks like a class name, treat it as a method filter.
        def lastDot = filter.lastIndexOf('.')
        String classPattern
        String methodName = null

        if (lastDot > 0) {
            def afterDot = filter.substring(lastDot + 1)
            def beforeDot = filter.substring(0, lastDot)
            if (afterDot.length() > 0 && Character.isLowerCase(afterDot.charAt(0))
                    && !afterDot.contains('*')
                    && looksLikeClassName(beforeDot)) {
                classPattern = beforeDot
                methodName = afterDot
            } else {
                classPattern = filter
            }
        } else {
            classPattern = filter
        }

        def matched = matchClasses(classPattern)
        if (matched.isEmpty()) {
            throw new GradleException(
                "No tests found matching filter '${filter}' in task '${name}'. " +
                "Known classes: ${testClasses.collect { it.substring(it.lastIndexOf('.') + 1) }.join(', ')}")
        }

        return matched.collect { new Selection(className: it, methodName: methodName) }
    }

    List<String> matchClasses(String pattern) {
        if (pattern.contains('*')) {
            def regex = pattern.replace('.', '\\.').replace('*', '.*')
            return testClasses.findAll { fqcn ->
                fqcn.matches(regex) || fqcn.substring(fqcn.lastIndexOf('.') + 1).matches(regex)
            }
        } else if (pattern.contains('.')) {
            return testClasses.findAll { it == pattern }
        } else {
            return testClasses.findAll { it.substring(it.lastIndexOf('.') + 1) == pattern }
        }
    }

    /** Last segment starts with uppercase? */
    static boolean looksLikeClassName(String s) {
        def lastDot = s.lastIndexOf('.')
        def lastSegment = lastDot >= 0 ? s.substring(lastDot + 1) : s
        return lastSegment.length() > 0 && Character.isUpperCase(lastSegment.charAt(0))
    }

    static class Selection {
        String className
        String methodName
    }
}
