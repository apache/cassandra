import spock.lang.Specification
import spock.lang.TempDir
import java.nio.file.Path

class AntBuildPropertiesTest extends Specification {

    @TempDir
    Path tmp

    def 'parses simple property elements'() {
        given:
        def xml = tmp.resolve('build.xml').toFile()
        xml.text = '''\
            <project name="test">
                <property name="foo" value="bar"/>
                <property name="baz" value="qux"/>
            </project>
        '''.stripIndent()

        when:
        def props = AntBuildProperties.parse(xml)

        then:
        props['foo'] == 'bar'
        props['baz'] == 'qux'
    }

    def 'first definition wins (Ant immutability)'() {
        given:
        def xml = tmp.resolve('build.xml').toFile()
        xml.text = '''\
            <project name="test">
                <property name="key" value="first"/>
                <property name="key" value="second"/>
            </project>
        '''.stripIndent()

        when:
        def props = AntBuildProperties.parse(xml)

        then:
        props['key'] == 'first'
    }

    def 'ignores property elements without name'() {
        given:
        def xml = tmp.resolve('build.xml').toFile()
        xml.text = '''\
            <project name="test">
                <property environment="env"/>
                <property name="real" value="yes"/>
            </project>
        '''.stripIndent()

        when:
        def props = AntBuildProperties.parse(xml)

        then:
        props['real'] == 'yes'
        props.size() == 1
    }

    def 'parses real Cassandra build.xml'() {
        given:
        // locate the real build.xml relative to project root
        def rootDir = new File(System.getProperty('user.dir')).parentFile
        def buildXml = new File(rootDir, 'build.xml')

        when:
        def props = AntBuildProperties.parse(buildXml)

        then:
        // Spot-check a few well-known properties
        props['base.version'] != null
        props['asm.version'] != null
        props['jamm.version'] != null
    }
}
