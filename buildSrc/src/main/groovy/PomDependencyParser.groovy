import groovy.xml.XmlSlurper

/**
 * Parses Maven POM files from the .build/ directory, resolving {@code @token@} Ant tokens
 * and {@code ${property}} Maven property references.
 */
class PomDependencyParser {

    private final Map<String, String> antTokens

    PomDependencyParser(Map<String, String> antTokens) {
        this.antTokens = antTokens ?: [:]
    }

    /**
     * Inner class representing a resolved Maven dependency.
     */
    static class Dependency {
        String groupId
        String artifactId
        String version
        String scope
        String classifier
        String type

        String toGradleNotation() {
            def base = "${groupId}:${artifactId}:${version}"
            if (classifier) {
                base += ":${classifier}"
            }
            return base
        }

        @Override
        String toString() {
            "Dependency{${toGradleNotation()}, scope=${scope ?: 'compile'}, type=${type ?: 'jar'}}"
        }
    }

    /**
     * Parses the parent POM's {@code <dependencyManagement>} section to build a version map.
     * Also reads {@code <properties>} for Maven property resolution.
     *
     * @return a map of "groupId:artifactId" → Dependency (with version and scope from dependencyManagement)
     */
    Map<String, Dependency> parseParentVersions(File parentPom) {
        def xml = new XmlSlurper().parse(parentPom)
        def ns = xml.lookupNamespace('')

        // Collect Maven <properties>
        Map<String, String> mavenProps = [:]
        xml.'**'.findAll { it.name() == 'properties' }.each { propsNode ->
            propsNode.children().each { child ->
                String propName = child.name()
                String propValue = substituteTokens(child.text())
                mavenProps[propName] = propValue
            }
        }

        Map<String, Dependency> versions = [:]

        // Parse <dependencyManagement><dependencies>
        xml.'**'.findAll { it.name() == 'dependencyManagement' }.each { dm ->
            dm.'**'.findAll { it.name() == 'dependency' }.each { dep ->
                String groupId = dep.groupId.text()
                String artifactId = dep.artifactId.text()
                String version = resolveProperties(substituteTokens(dep.version.text()), mavenProps)
                String scope = dep.scope.text() ?: null
                String classifier = dep.classifier.text() ?: null
                String type = dep.type.text() ?: null

                if (groupId && artifactId) {
                    String key = "${groupId}:${artifactId}"
                    // If there's a classifier, use a more specific key
                    if (classifier) {
                        key = "${key}:${classifier}"
                    }
                    if (!versions.containsKey(key)) {
                        versions[key] = new Dependency(
                            groupId: groupId,
                            artifactId: artifactId,
                            version: version,
                            scope: scope,
                            classifier: classifier,
                            type: type
                        )
                    }
                }
            }
        }

        return versions
    }

    /**
     * Parses a child POM's {@code <dependencies>} section, resolving versions from the parent version map.
     *
     * @param childPom the child POM file
     * @param parentVersions the version map from parseParentVersions
     * @return a list of resolved Dependencies
     */
    List<Dependency> parseDependencies(File childPom, Map<String, Dependency> parentVersions) {
        def xml = new XmlSlurper().parse(childPom)
        List<Dependency> deps = []

        // Only parse top-level <dependencies>, not <dependencyManagement> or <profile> deps
        xml.dependencies.dependency.each { dep ->
            String groupId = dep.groupId.text()
            String artifactId = dep.artifactId.text()
            String version = dep.version.text() ?: null
            String scope = dep.scope.text() ?: null
            String classifier = dep.classifier.text() ?: null
            String type = dep.type.text() ?: null

            // Resolve version from parent if not specified in child
            String key = "${groupId}:${artifactId}"
            if (classifier) {
                String classifiedKey = "${key}:${classifier}"
                if (parentVersions.containsKey(classifiedKey)) {
                    def parent = parentVersions[classifiedKey]
                    if (!version) version = parent.version
                    if (!scope) scope = parent.scope
                } else if (parentVersions.containsKey(key)) {
                    def parent = parentVersions[key]
                    if (!version) version = parent.version
                    if (!scope) scope = parent.scope
                }
            } else if (parentVersions.containsKey(key)) {
                def parent = parentVersions[key]
                if (!version) version = parent.version
                if (!scope) scope = parent.scope
            }

            version = substituteTokens(version ?: '')

            deps << new Dependency(
                groupId: groupId,
                artifactId: artifactId,
                version: version,
                scope: scope,
                classifier: classifier,
                type: type
            )
        }

        return deps
    }

    /**
     * Replace {@code @token@} patterns with values from the Ant properties map.
     */
    private String substituteTokens(String input) {
        if (!input) return input
        input.replaceAll(/@([^@]+)@/) { match, token ->
            antTokens.containsKey(token) ? antTokens[token] : match[0]
        }
    }

    /**
     * Resolve {@code ${property}} references using the Maven properties map.
     */
    private static String resolveProperties(String input, Map<String, String> mavenProps) {
        if (!input) return input
        input.replaceAll(/\$\{([^}]+)\}/) { match, prop ->
            mavenProps.containsKey(prop) ? mavenProps[prop] : match[0]
        }
    }
}
