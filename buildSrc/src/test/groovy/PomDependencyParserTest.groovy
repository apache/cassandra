import spock.lang.Specification
import spock.lang.TempDir
import java.nio.file.Path

class PomDependencyParserTest extends Specification {

    @TempDir
    Path tmp

    def 'parseParentVersions reads dependencyManagement versions'() {
        given:
        def pom = tmp.resolve('parent.xml').toFile()
        pom.text = '''\
            <project xmlns="http://maven.apache.org/POM/4.0.0">
              <dependencyManagement>
                <dependencies>
                  <dependency>
                    <groupId>com.google.guava</groupId>
                    <artifactId>guava</artifactId>
                    <version>32.0.1-jre</version>
                  </dependency>
                  <dependency>
                    <groupId>junit</groupId>
                    <artifactId>junit</artifactId>
                    <version>4.12</version>
                    <scope>test</scope>
                  </dependency>
                </dependencies>
              </dependencyManagement>
            </project>
        '''.stripIndent()

        def parser = new PomDependencyParser([:])

        when:
        def versions = parser.parseParentVersions(pom)

        then:
        versions['com.google.guava:guava'].version == '32.0.1-jre'
        versions['junit:junit'].version == '4.12'
        versions['junit:junit'].scope == 'test'
    }

    def 'substituteTokens replaces @token@ patterns'() {
        given:
        def pom = tmp.resolve('parent.xml').toFile()
        pom.text = '''\
            <project xmlns="http://maven.apache.org/POM/4.0.0">
              <dependencyManagement>
                <dependencies>
                  <dependency>
                    <groupId>com.github.jbellis</groupId>
                    <artifactId>jamm</artifactId>
                    <version>@jamm.version@</version>
                  </dependency>
                </dependencies>
              </dependencyManagement>
            </project>
        '''.stripIndent()

        def parser = new PomDependencyParser(['jamm.version': '0.4.0'])

        when:
        def versions = parser.parseParentVersions(pom)

        then:
        versions['com.github.jbellis:jamm'].version == '0.4.0'
    }

    def 'resolves Maven ${property} references'() {
        given:
        def pom = tmp.resolve('parent.xml').toFile()
        pom.text = '''\
            <project xmlns="http://maven.apache.org/POM/4.0.0">
              <properties>
                <netty.version>4.1.130.Final</netty.version>
              </properties>
              <dependencyManagement>
                <dependencies>
                  <dependency>
                    <groupId>io.netty</groupId>
                    <artifactId>netty-all</artifactId>
                    <version>${netty.version}</version>
                  </dependency>
                </dependencies>
              </dependencyManagement>
            </project>
        '''.stripIndent()

        def parser = new PomDependencyParser([:])

        when:
        def versions = parser.parseParentVersions(pom)

        then:
        versions['io.netty:netty-all'].version == '4.1.130.Final'
    }

    def 'parseDependencies resolves versions from parent map'() {
        given:
        def childPom = tmp.resolve('child.xml').toFile()
        childPom.text = '''\
            <project xmlns="http://maven.apache.org/POM/4.0.0">
              <dependencies>
                <dependency>
                  <groupId>com.google.guava</groupId>
                  <artifactId>guava</artifactId>
                </dependency>
                <dependency>
                  <groupId>junit</groupId>
                  <artifactId>junit</artifactId>
                </dependency>
              </dependencies>
            </project>
        '''.stripIndent()

        def parentVersions = [
            'com.google.guava:guava': new PomDependencyParser.Dependency(
                groupId: 'com.google.guava', artifactId: 'guava',
                version: '32.0.1-jre', scope: null),
            'junit:junit': new PomDependencyParser.Dependency(
                groupId: 'junit', artifactId: 'junit',
                version: '4.12', scope: 'test')
        ]

        def parser = new PomDependencyParser([:])

        when:
        def deps = parser.parseDependencies(childPom, parentVersions)

        then:
        deps.size() == 2
        deps[0].toGradleNotation() == 'com.google.guava:guava:32.0.1-jre'
        deps[1].toGradleNotation() == 'junit:junit:4.12'
        deps[1].scope == 'test'
    }

    def 'parseDependencies handles classifier'() {
        given:
        def childPom = tmp.resolve('child.xml').toFile()
        childPom.text = '''\
            <project xmlns="http://maven.apache.org/POM/4.0.0">
              <dependencies>
                <dependency>
                  <groupId>org.apache.cassandra</groupId>
                  <artifactId>cassandra-driver-core</artifactId>
                  <classifier>shaded</classifier>
                </dependency>
              </dependencies>
            </project>
        '''.stripIndent()

        def parentVersions = [
            'org.apache.cassandra:cassandra-driver-core': new PomDependencyParser.Dependency(
                groupId: 'org.apache.cassandra', artifactId: 'cassandra-driver-core',
                version: '3.12.1', scope: null)
        ]

        def parser = new PomDependencyParser([:])

        when:
        def deps = parser.parseDependencies(childPom, parentVersions)

        then:
        deps.size() == 1
        deps[0].toGradleNotation() == 'org.apache.cassandra:cassandra-driver-core:3.12.1:shaded'
        deps[0].classifier == 'shaded'
    }

    def 'parses real Cassandra parent POM'() {
        given:
        def rootDir = new File(System.getProperty('user.dir')).parentFile
        def buildXml = new File(rootDir, 'build.xml')
        def parentPom = new File(rootDir, '.build/parent-maven-pom.xml')
        def antTokens = AntBuildProperties.parse(buildXml)
        def parser = new PomDependencyParser(antTokens)

        when:
        def versions = parser.parseParentVersions(parentPom)

        then:
        // Spot-check versions that use different resolution strategies
        versions['com.google.guava:guava'].version == '32.0.1-jre'        // literal
        versions['io.netty:netty-all'].version == '4.1.130.Final'         // ${property}
        versions['com.github.jbellis:jamm'].version == '0.4.0'            // @token@
        versions['org.ow2.asm:asm'].version == '9.5'                      // @token@ → ${property}
    }

    def 'parses real Cassandra deps POM with parent resolution'() {
        given:
        def rootDir = new File(System.getProperty('user.dir')).parentFile
        def buildXml = new File(rootDir, 'build.xml')
        def parentPom = new File(rootDir, '.build/parent-maven-pom.xml')
        def depsPom = new File(rootDir, '.build/cassandra-deps-maven-pom.xml')
        def antTokens = AntBuildProperties.parse(buildXml)
        def parser = new PomDependencyParser(antTokens)

        when:
        def parentVersions = parser.parseParentVersions(parentPom)
        def deps = parser.parseDependencies(depsPom, parentVersions)

        then:
        deps.size() > 0
        // Find guava and verify resolution
        def guava = deps.find { it.artifactId == 'guava' }
        guava != null
        guava.version == '32.0.1-jre'
        // Find netty-all
        def netty = deps.find { it.artifactId == 'netty-all' }
        netty != null
        netty.version == '4.1.130.Final'
    }
}
