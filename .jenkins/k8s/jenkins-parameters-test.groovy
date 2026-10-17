// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import groovy.json.JsonSlurper
import groovy.xml.XmlUtil
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.customizers.ImportCustomizer

// Execute the checked-in scripts with in-memory job storage and a small Job DSL adapter.
// This checks parameter XML across reloads without a Jenkins controller or build agents.
class SeedJob {
    Node xml = new Node(null, 'flow-definition')
    List<Closure> configureBlocks = []
    String scmBranch

    SeedJob() { xml.appendNode('properties') }

    void runContext(Closure body) {
        def action = body.rehydrate(this, body.owner, body.thisObject)
        action.resolveStrategy = Closure.DELEGATE_FIRST
        action()
    }

    void parameters(Closure body) {
        xml.properties[0].appendNode('hudson.model.ParametersDefinitionProperty').appendNode('parameterDefinitions')
        runContext(body)
    }

    void stringParam(String name, String value, String description) {
        def node = definitions().appendNode('hudson.model.StringParameterDefinition')
        node.appendNode('name', name)
        node.appendNode('description', description)
        node.appendNode('defaultValue', value)
    }

    void choiceParam(String name, List values, String description) {
        def node = definitions().appendNode('hudson.model.ChoiceParameterDefinition')
        node.appendNode('name', name)
        node.appendNode('description', description)
        def choices = node.appendNode('choices', [class: 'java.util.Arrays$ArrayList'])
                          .appendNode('a', [class: 'string-array'])
        values.each { choices.appendNode('string', it) }
    }

    Node definitions() { xml.properties[0].'hudson.model.ParametersDefinitionProperty'[0].parameterDefinitions[0] }
    void configure(Closure body) { configureBlocks.add(body) }

    def methodMissing(String name, args) {
        if (name in ['definition', 'cpsScm', 'scm', 'git', 'remote']) {
            runContext(args[0])
        } else if (name == 'branch') {
            scmBranch = args[0]
        } else if (!(name in ['url', 'scriptPath', 'lightweight'])) {
            throw new MissingMethodException(name, getClass(), args)
        }
    }

    Node render() {
        configureBlocks.each { it.call(xml) }
        xml.appendNode('definition').appendNode('scm').appendNode('branch', scmBranch)
        xml
    }
}

class ParameterDsl {
    void agent(Closure ignored) {}
    void options(Closure ignored) {}
    void stages(Closure ignored) {}
    void post(Closure ignored) {}
    void parameters(Closure body) { body.call() }
}

// Jenkins uses Groovy 2; the existing validation container uses Groovy 4.
def xmlParserName = GroovySystem.version.startsWith('2.') ? 'groovy.util.XmlParser' : 'groovy.xml.XmlParser'
def xmlParserClass = getClass().classLoader.loadClass(xmlParserName)
def parseXml = { text -> xmlParserClass.newInstance().parseText(text) }
def compiler = new CompilerConfiguration()
compiler.addCompilationCustomizers(new ImportCustomizer().addImports(xmlParserName))
def loader = new GroovyClassLoader(getClass().classLoader)
def missingConfig = loader.parseClass('''
    package javaposse.jobdsl.dsl
    class JobConfigurationNotFoundException extends RuntimeException {}
''')
loader.parseClass('@interface NonCPS {}')
loader.parseClass('package hudson; class AbortException extends IOException {}')
def scripts = new JsonSlurper().parse(new File(args[0]))
def store = [:]
def readFailure = null
def reload = {
    def generated = [:]
    def management = new Expando(getConfig: { String name ->
        if (readFailure != null) throw readFailure
        if (!store.containsKey(name)) throw missingConfig.newInstance()
        store[name]
    })
    scripts.each { source ->
        def binding = new Binding(jm: management, pipelineJob: { String name, Closure body ->
            def job = new SeedJob()
            job.runContext(body)
            generated[name] = job
        })
        new GroovyShell(loader, binding, compiler).evaluate(source)
    }
    generated.each { name, job -> store[name] = XmlUtil.serialize(job.render()) }
}
def definitions = { String name ->
    parseXml(store[name]).properties.'hudson.model.ParametersDefinitionProperty'.parameterDefinitions[0]
}
def parameter = { String job, String name -> definitions(job)?.children()?.find { it.name.text() == name } }
def choices = { Node definition -> definition == null ? [] : definition.choices[0].depthFirst().findAll { it instanceof Node && it.name() == 'string' }*.text() }
def failures = []
def check = { String name, Closure test ->
    try {
        test()
        println "PASS ${name}"
    } catch (AssertionError error) {
        failures.add(name)
        println "FAIL ${name}: ${error.message}"
    }
}

reload()
check('fresh jobs have branch and profile defaults before their first build') {
    ['cassandra': 'trunk', 'cassandra-6.0': 'cassandra-6.0', 'cassandra-5.0': 'cassandra-5.0'].each { job, branch ->
        assert parameter(job, 'branch')?.defaultValue?.text() == branch
        assert choices(parameter(job, 'profile'))[0] == 'skinny'
        assert definitions(job).children().size() == 8
        assert choices(parameter(job, 'architecture')) == ['amd64', 'arm64', 'all']
    }
}

// A build can update defaults and add parameter definitions unknown to the seed script.
store['cassandra'] = '''
<flow-definition><properties><hudson.model.ParametersDefinitionProperty><parameterDefinitions>
  <hudson.model.StringParameterDefinition><name>branch</name><defaultValue>feature/CASSANDRA-123</defaultValue></hudson.model.StringParameterDefinition>
  <hudson.model.ChoiceParameterDefinition><name>profile</name><choices><string>pre-commit</string><string>skinny</string><string>custom</string></choices></hudson.model.ChoiceParameterDefinition>
  <hudson.model.StringParameterDefinition><name>repository</name><defaultValue>https://github.com/example/cassandra</defaultValue></hudson.model.StringParameterDefinition>
  <hudson.model.BooleanParameterDefinition><name>extra</name><defaultValue>true</defaultValue></hudson.model.BooleanParameterDefinition>
</parameterDefinitions></hudson.model.ParametersDefinitionProperty></properties>
<definition><scm><branch>old-definition</branch></scm></definition></flow-definition>
'''
2.times { reload() }
check('repeated reloads preserve saved defaults and extra parameters while updating SCM') {
    assert parameter('cassandra', 'branch')?.defaultValue?.text() == 'feature/CASSANDRA-123'
    assert choices(parameter('cassandra', 'profile')) == ['pre-commit', 'skinny', 'custom']
    assert parameter('cassandra', 'repository')?.defaultValue?.text() == 'https://github.com/example/cassandra'
    assert parameter('cassandra', 'extra')?.defaultValue?.text() == 'true'
    assert definitions('cassandra').children().size() == 9
    assert parseXml(store.cassandra).definition.scm.branch.text() == 'trunk'
    assert parameter('cassandra-5.0', 'branch')?.defaultValue?.text() == 'cassandra-5.0'
}

store['cassandra'] = store['cassandra'].replace('feature/CASSANDRA-123', '   ').replace('<string>pre-commit</string>', '<string></string>')
store['cassandra-6.0'] = '<flow-definition><properties/></flow-definition>'
reload()
check('reload repairs blank defaults and jobs with no parameter property') {
    assert parameter('cassandra', 'branch')?.defaultValue?.text() == 'trunk'
    assert choices(parameter('cassandra', 'profile'))[0] == 'skinny'
    assert parameter('cassandra-6.0', 'branch')?.defaultValue?.text() == 'cassandra-6.0'
    assert choices(parameter('cassandra-6.0', 'profile'))[0] == 'skinny'
    assert parameter('cassandra', 'extra')?.defaultValue?.text() == 'true'
}

check('a config read failure cannot replace saved parameters with defaults') {
    def before = store.clone()
    readFailure = new SecurityException('read denied')
    try {
        reload()
        assert false: 'reload must fail when existing configuration cannot be read'
    } catch (SecurityException expected) {
        assert store == before
    } finally {
        readFailure = null
    }
}

// Capture just the real Jenkinsfile's parameter declarations; never execute build stages.
def pipelineParameters = { Map params, String branch ->
    def declarations = [:]
    def script = new GroovyShell(loader, new Binding(params: params,
        scm: [branch: null, branches: [[name: branch]], userRemoteConfigs: [[url: 'https://github.com/apache/cassandra']]]), compiler)
        .parse(new File(args[1]).text, 'CassandraPipeline.groovy')
    script.metaClass.pipeline = { Closure body ->
        def dsl = new ParameterDsl()
        def action = body.rehydrate(dsl, script, script)
        action.resolveStrategy = Closure.DELEGATE_FIRST
        action()
    }
    script.metaClass.string = { Map definition -> declarations[definition.name] = definition }
    script.metaClass.choice = { Map definition -> declarations[definition.name] = definition }
    script.run()
    declarations
}
check('Jenkinsfile has nonblank defaults with missing or blank parameters') {
    [[:], [branch: '', profile: ''], [branch: '  ', profile: '  ']].each { params ->
        def result = pipelineParameters(params, '*/cassandra-6.0')
        assert result.branch.defaultValue == 'cassandra-6.0'
        assert result.profile.choices[0] == 'skinny'
        assert result.profile.choices.every { it.trim() }
        assert result.profile.choices.unique(false) == result.profile.choices
    }
}
check('Jenkinsfile retains a selected branch and valid profile') {
    def result = pipelineParameters([branch: 'feature/CASSANDRA-456', profile: 'pre-commit'], 'trunk')
    assert result.branch.defaultValue == 'feature/CASSANDRA-456'
    assert result.profile.choices[0] == 'pre-commit'
}
check('seeded defaults remain selected when the Jenkinsfile defines parameters') {
    ['cassandra', 'cassandra-6.0', 'cassandra-5.0'].each { job ->
        def result = pipelineParameters([
            branch: parameter(job, 'branch')?.defaultValue?.text(),
            profile: choices(parameter(job, 'profile'))[0]
        ], parseXml(store[job]).definition.scm.branch.text())
        assert result.branch.defaultValue == (job == 'cassandra' ? 'trunk' : job)
        assert result.profile.choices[0] == 'skinny'
        assert result.profile.choices.toSet() == choices(parameter(job, 'profile')).toSet()
    }
}
check('Jenkinsfile replaces a removed profile with skinny') {
    assert pipelineParameters([profile: 'removed-profile'], 'trunk').profile.choices[0] == 'skinny'
}
assert failures.empty: "${failures.size()} parameter regression(s) failed: ${failures.join(', ')}"
