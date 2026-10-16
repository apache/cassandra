import groovy.xml.XmlSlurper

/**
 * Parses {@code <property name="X" value="Y"/>} elements from an Ant build.xml file.
 * First-definition-wins (mimics Ant immutability).
 */
class AntBuildProperties {

    /**
     * Parse all {@code <property name="..." value="..."/>} elements from the given XML file.
     * First definition wins (Ant immutability semantics).
     */
    static Map<String, String> parse(File buildXml) {
        def xml = new XmlSlurper().parse(buildXml)
        Map<String, String> props = [:]
        xml.property.each { node ->
            String name = node.@name.text()
            String value = node.@value.text()
            if (name && !props.containsKey(name)) {
                props[name] = value
            }
        }
        return props
    }
}
