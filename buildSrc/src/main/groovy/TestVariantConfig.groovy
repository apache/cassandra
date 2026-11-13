/**
 * Data class representing a test variant (compression, cdc, latest, oa, system-keyspace-directory).
 * Each variant may overlay additional YAML configuration and inject extra system properties.
 */
class TestVariantConfig {

    /** Human-readable variant name (e.g. "compression") */
    final String name

    /** YAML overlay files to concatenate on top of test/conf/cassandra.yaml */
    final List<String> overlayYamls

    /** Extra system properties to pass to the test JVM */
    final Map<String, String> systemProperties

    TestVariantConfig(String name, List<String> overlayYamls, Map<String, String> systemProperties) {
        this.name = name
        this.overlayYamls = overlayYamls
        this.systemProperties = systemProperties
    }

    /**
     * Returns all known test variants keyed by name.
     */
    static Map<String, TestVariantConfig> all() {
        return [
            'compression': new TestVariantConfig('compression',
                ['test/conf/commitlog_compression_LZ4.yaml'],
                [
                    'cassandra.test.compression'     : 'true',
                    'cassandra.test.compression.algo' : 'LZ4',
                    'cassandra.tolerate_sstable_size' : 'true',
                ]),
            'cdc': new TestVariantConfig('cdc',
                ['test/conf/cdc.yaml'],
                [
                    'cassandra.tolerate_sstable_size': 'true',
                ]),
            'latest': new TestVariantConfig('latest',
                ['test/conf/latest_diff.yaml', 'test/conf/storage_compatibility_mode_none.yaml'],
                [
                    'cassandra.tolerate_sstable_size'             : 'true',
                    'cassandra.test.storage_compatibility_mode'   : 'NONE',
                ]),
            'oa': new TestVariantConfig('oa',
                ['test/conf/storage_compatibility_mode_none.yaml'],
                [
                    'cassandra.tolerate_sstable_size'             : 'true',
                    'cassandra.test.storage_compatibility_mode'   : 'NONE',
                    'cassandra.cursor_compaction_enabled'         : 'false',
                ]),
            'system-keyspace-directory': new TestVariantConfig('system-keyspace-directory',
                ['test/conf/system_keyspaces_directory.yaml'],
                [
                    'cassandra.tolerate_sstable_size': 'true',
                ]),
        ]
    }
}
