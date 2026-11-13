/**
 * Provides JDK-version-specific JVM arguments for compilation, runtime, and testing.
 * Mirrors the flag lists from build.xml ({@code _jvm11_arg_items}, {@code _jvm17_arg_items}, etc.).
 */
class JdkJvmArgs {

    /**
     * {@code --add-exports} flags required by javac (from build.xml {@code jdk11plus-javac-exports}).
     */
    static List<String> javacExports() {
        return [
            '--add-exports', 'java.rmi/sun.rmi.registry=ALL-UNNAMED',
            '--add-exports', 'java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED',
            '--add-exports', 'java.base/jdk.internal.ref=ALL-UNNAMED',
            '--add-exports', 'java.base/sun.nio.ch=ALL-UNNAMED',
            '--add-exports', 'java.management/com.sun.jmx.remote.security=ALL-UNNAMED',
        ]
    }

    /**
     * JDK-specific {@code --add-exports} / {@code --add-opens} for runtime JVM args.
     * Mirrors {@code _jvm11_arg_items}, {@code _jvm17_arg_items}, {@code _jvm21_arg_items}.
     */
    static List<String> jvmArgs(int jdkVersion) {
        switch (jdkVersion) {
            case 11: return jvm11Args()
            case 17: return jvm17Args()
            case 21: return jvm21Args()
            default:
                // For unknown/newer JDKs, fall back to JDK 21 args
                return jvm21Args()
        }
    }

    /**
     * JDK-specific test JVM args.
     * Mirrors {@code _jvm11_test_arg_items}, {@code _jvm17_test_arg_items}, {@code _jvm21_test_arg_items}.
     */
    static List<String> testArgs(int jdkVersion) {
        switch (jdkVersion) {
            case 11: return jvm11TestArgs()
            case 17: return jvm17TestArgs()
            case 21: return jvm21TestArgs()
            default:
                return jvm21TestArgs()
        }
    }

    /**
     * Detect the major version of the running JDK.
     */
    static int detectJdkVersion() {
        String specVersion = System.getProperty('java.specification.version')
        if (specVersion.contains('.')) {
            // Pre-JDK 9 format: "1.8"
            return specVersion.split('\\.')[1] as int
        }
        return specVersion as int
    }

    // -------------------------------------------------------------------------
    // JDK 11 runtime args (from build.xml _jvm11_arg_items)
    // -------------------------------------------------------------------------
    private static List<String> jvm11Args() {
        return [
            '-Djdk.attach.allowAttachSelf=true',
            '-XX:+UseConcMarkSweepGC',
            '-XX:+CMSParallelRemarkEnabled',
            '-XX:SurvivorRatio=8',
            '-XX:MaxTenuringThreshold=1',
            '-XX:CMSInitiatingOccupancyFraction=75',
            '-XX:+UseCMSInitiatingOccupancyOnly',
            '-XX:CMSWaitDuration=10000',
            '-XX:+CMSParallelInitialMarkEnabled',
            '-XX:+CMSEdenChunksRecordAlways',

            '--add-exports', 'java.base/jdk.internal.misc=ALL-UNNAMED',
            '--add-exports', 'java.base/jdk.internal.ref=ALL-UNNAMED',
            '--add-exports', 'java.base/sun.nio.ch=ALL-UNNAMED',
            '--add-exports', 'java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED',
            '--add-exports', 'java.rmi/sun.rmi.registry=ALL-UNNAMED',
            '--add-exports', 'java.rmi/sun.rmi.server=ALL-UNNAMED',
            '--add-exports', 'java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED',
            '--add-exports', 'java.sql/java.sql=ALL-UNNAMED',

            '--add-opens', 'java.base/java.lang.module=ALL-UNNAMED',
            '--add-opens', 'java.base/java.net=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.loader=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.ref=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.reflect=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.math=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.module=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.util.jar=ALL-UNNAMED',
            '--add-opens', 'jdk.management/com.sun.management.internal=ALL-UNNAMED',
        ]
    }

    // -------------------------------------------------------------------------
    // JDK 17 runtime args (from build.xml _jvm17_arg_items)
    // -------------------------------------------------------------------------
    private static List<String> jvm17Args() {
        return [
            '-Djdk.attach.allowAttachSelf=true',
            '-XX:+UseG1GC',
            '-XX:+ParallelRefProcEnabled',
            '-XX:MaxTenuringThreshold=1',
            '-XX:G1HeapRegionSize=16m',

            '--add-exports', 'java.base/jdk.internal.misc=ALL-UNNAMED',
            '--add-exports', 'java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED',
            '--add-exports', 'java.rmi/sun.rmi.registry=ALL-UNNAMED',
            '--add-exports', 'java.rmi/sun.rmi.server=ALL-UNNAMED',
            '--add-exports', 'java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED',
            '--add-exports', 'java.sql/java.sql=ALL-UNNAMED',
            '--add-exports', 'java.base/java.lang.ref=ALL-UNNAMED',
            '--add-exports', 'jdk.unsupported/sun.misc=ALL-UNNAMED',
            '--add-exports', 'jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED',
            '--add-exports', 'jdk.attach/sun.tools.attach=ALL-UNNAMED',

            '--add-opens', 'java.base/java.lang.module=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.loader=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.ref=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.reflect=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.math=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.module=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.util.jar=ALL-UNNAMED',
            '--add-opens', 'jdk.management/com.sun.management.internal=ALL-UNNAMED',
            '--add-opens', 'java.base/sun.nio.ch=ALL-UNNAMED',
            '--add-opens', 'java.base/java.io=ALL-UNNAMED',
            '--add-opens', 'java.base/java.lang.reflect=ALL-UNNAMED',
            '--add-opens', 'jdk.compiler/com.sun.tools.javac=ALL-UNNAMED',
            '--add-opens', 'java.base/java.lang=ALL-UNNAMED',
            '--add-opens', 'java.base/java.util=ALL-UNNAMED',
            '--add-opens', 'java.base/java.nio=ALL-UNNAMED',
            '--add-opens', 'java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED',

            // Needed for in-jvm dtests straddling 6.0+
            '--add-opens', 'java.base/java.util.concurrent=ALL-UNNAMED',
            '--add-opens', 'java.base/java.util.concurrent.atomic=ALL-UNNAMED',
        ]
    }

    // -------------------------------------------------------------------------
    // JDK 21 runtime args (from build.xml _jvm21_arg_items)
    // -------------------------------------------------------------------------
    private static List<String> jvm21Args() {
        return [
            '-Djdk.attach.allowAttachSelf=true',

            '-XX:+UseZGC',
            '-XX:+ZGenerational',

            // Temporary workaround for jamm having incorrect default CompressedOops for JDK21
            '-XX:-UseCompressedOops',

            // Need to explicitly allow security manager on JDK21; deprecated for removal
            '-Djava.security.manager=allow',

            '--add-exports', 'java.base/java.lang.ref=ALL-UNNAMED',
            '--add-exports', 'java.base/java.lang.reflect=ALL-UNNAMED',
            '--add-exports', 'java.base/jdk.internal.misc=ALL-UNNAMED',
            '--add-exports', 'java.base/jdk.internal.ref=ALL-UNNAMED',
            '--add-exports', 'java.base/sun.nio.ch=ALL-UNNAMED',
            '--add-exports', 'java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED',
            '--add-exports', 'java.rmi/sun.rmi.registry=ALL-UNNAMED',
            '--add-exports', 'java.rmi/sun.rmi.server=ALL-UNNAMED',
            '--add-exports', 'java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED',
            '--add-exports', 'java.sql/java.sql=ALL-UNNAMED',
            '--add-exports', 'jdk.unsupported/sun.misc=ALL-UNNAMED',
            '--add-exports', 'jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED',
            '--add-exports', 'jdk.attach/sun.tools.attach=ALL-UNNAMED',

            '--add-opens', 'java.base/java.io=ALL-UNNAMED',
            '--add-opens', 'java.base/java.lang=ALL-UNNAMED',
            '--add-opens', 'java.base/java.lang.module=ALL-UNNAMED',
            '--add-opens', 'java.base/java.lang.reflect=ALL-UNNAMED',
            '--add-opens', 'java.base/java.math=ALL-UNNAMED',
            '--add-opens', 'java.base/java.net=ALL-UNNAMED',
            '--add-opens', 'java.base/java.nio=ALL-UNNAMED',
            '--add-opens', 'java.base/java.util=ALL-UNNAMED',
            '--add-opens', 'java.base/java.util.concurrent=ALL-UNNAMED',
            '--add-opens', 'java.base/java.util.concurrent.atomic=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.loader=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.math=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.module=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.ref=ALL-UNNAMED',
            '--add-opens', 'java.base/jdk.internal.reflect=ALL-UNNAMED',
            '--add-opens', 'java.base/sun.nio.ch=ALL-UNNAMED',
            '--add-opens', 'java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED',
            '--add-opens', 'jdk.management/com.sun.management=ALL-UNNAMED',
        ]
    }

    // -------------------------------------------------------------------------
    // JDK 11 test args (from build.xml _jvm11_test_arg_items)
    // -------------------------------------------------------------------------
    private static List<String> jvm11TestArgs() {
        return [
            '-XX:+HeapDumpOnOutOfMemoryError',
            '-XX:-CMSClassUnloadingEnabled',
            '-Dio.netty.tryReflectionSetAccessible=true',
            '-XX:MaxMetaspaceSize=2G',
        ]
    }

    // -------------------------------------------------------------------------
    // JDK 17 test args (from build.xml _jvm17_test_arg_items)
    // -------------------------------------------------------------------------
    private static List<String> jvm17TestArgs() {
        return [
            '-XX:+HeapDumpOnOutOfMemoryError',
            '-Dio.netty.tryReflectionSetAccessible=true',
            '--add-opens=java.base/java.lang=ALL-UNNAMED',
            '--add-exports=java.base/jdk.internal.vm.annotation=ALL-UNNAMED',
        ]
    }

    // -------------------------------------------------------------------------
    // JDK 21 test args (from build.xml _jvm21_test_arg_items)
    // -------------------------------------------------------------------------
    private static List<String> jvm21TestArgs() {
        return [
            '-XX:+HeapDumpOnOutOfMemoryError',
            '-Dnet.bytebuddy.experimental=true',
            '-Djava.security.manager=allow',
            '-Dio.netty.tryReflectionSetAccessible=true',
            '--add-opens=java.base/java.lang=ALL-UNNAMED',
            '--add-exports=java.base/jdk.internal.vm.annotation=ALL-UNNAMED',
            // Revert to pre-JEP-416 Core Reflection implementation (without method handles)
            '-Djdk.reflect.useDirectMethodHandle=false',
        ]
    }
}
