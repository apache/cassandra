<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

The goal of this document is to establish guidelines on writing tests that drive incremental improvement of the test coverage and testability of 
Cassandra, without overly burdening day to day work. While not every point here will be immediately practical to implement or relevant for every 
contribution, it errs on the side of not making rules out of potential exceptions. It provides guidelines on test scope, style, and goals, as
well as guidelines for dealing with global state and refactoring untested code.

## What to Test

There are 3 main types of tests in Cassandra, unit tests, integration tests, and dtests. Below, each type of test is described, and a high level description of 
what should and shouldn't be tested in each is given.

### Unit Tests
JUnit tests of smaller components that are fairly narrow in scope (ie: data structures, verb handlers, helper classes)

#### What should be tested
 * All state transitions should be tested
 * Illegal state transitions should be tested (that they throw exceptions)
 * all conditional branches should be tested. 
 * Code that deals with ranges of values should have tests around expected ranges, unexpected ranges, different functional ranges and their boundaries.
 * Exception handling should be tested.

#### What shouldn't be tested
* implementation details (test that the system under test works a certain way, not that it's implemented a certain way)

### Integration Tests
JUnit tests of larger components with a lot of moving parts, usually involved in some internode communication (ie: Gossip, MessagingService).
The smaller components that make up the system under test in an integration test should be individually unit tested.

#### What should be tested
* messages are sent when expected
* received messages have the intended side effects
* internal interfaces work as expected
* external interfaces are interacted with as expected
* multiple instances of components interact as expected (with a mocked messaging service, and other dependencies, where appropriate)
* dry start - test that a system starts up properly the first time a node start
* restart - test that a system starts up properly on node restart (from both clean and unclean shutdown)
* shutdown - test that a system can shutdown properly
* upgrade - test that a system is able to restart with data from a previous version

#### What shouldn't be tested
* The rest of the application. It should be possible to test large systems without starting the entire database through the use of mocks.

**Note:** it's generally not a good idea to mock out the storage layer if the component under test needs to interact with it. If you're testing
how multiple instances interact with each other, AND they need to use the storage layer, parameterizing the keyspace/table they store data is
the way to do it.

### dtests
python/ccm tests that start local clusters and interact with them via the python client.

dtests are effectively black box tests, and should be used for checking that clusters and client side interfaces work as expected. They are not 
a replacement for proper functional java tests. They take much longer to run, and are much less flexible in what they can test.

Systems under test in dtest should have more granular integration tests as well.

#### What should be tested
* end to end cluster functionality
* client contracts
* trivial (to create) failure cases

#### What shouldn't be tested
* internal implementation details

## Expanded Guidelines

This section has more in depth descriptions and reasoning about some of the points raised in the previous section.

### Test structure

Tests cases should have a clear progression of setup, precondition check, performing some action under test, then a postcondition check.

**Example**

```java
@Test
public void increment() throws Exception
{
	// setup code
	int x = 1;

	// check preconditions
	assertEquals(1, x);

	// perform the state changing action under test
	x++;

	// check post conditions
	assertEquals(2, x);
}
```

#### Reason

Test cases should be optimized for readability

#### Exceptions

Cases where simple cases can be tested in one line. Such as validation logic checks:
property-based state testing (ie: ScalaCheck/QuickCheck)

*Example:*
```java
@Test
public void validation()
{
	assertValidationFailure(b -> b.withState(null));
	assertValidationFailure(b -> b.withSessionID(null));
	assertValidationFailure(b -> b.withCoordinator(null));
	assertValidationFailure(b -> b.withTableIds(null));
	assertValidationFailure(b -> b.withTableIds(new HashSet<>()));
	assertValidationFailure(b -> b.withRepairedAt(0));
	assertValidationFailure(b -> b.withRepairedAt(-1));
	assertValidationFailure(b -> b.withRanges(null));
	assertValidationFailure(b -> b.withRanges(new HashSet<>()));
	assertValidationFailure(b -> b.withParticipants(null));
	assertValidationFailure(b -> b.withParticipants(new HashSet<>()));
	assertValidationFailure(b -> b.withStartedAt(0));
	assertValidationFailure(b -> b.withLastUpdate(0));
}
```

### Test distributed components in junit

Components that rely on nodes communicating with each other should be testable in java. 

#### Reason

One of the more difficult aspects of distributed systems is ensuring that they continue to behave correctly during various failure modes.  This includes weird 
edge cases involving specific ordering of events between nodes that rarely occur in the wild. Testing these sorts of scenarios is much easier in junit because 
mock 'clusters' can be placed in specific states, and deterministically stepped through a sequence of events, ensuring that they behave as expected, and are in 
the expected state after each step.

#### Exceptions

This rule mainly applies to new or significantly overhauled systems. Older systems *should* be refactored to be thoroughly tested, but it's not necessarily a 
prerequisite for working on them.

### Test all branches and inputs.

All branches and inputs of a method should be exercised. For branches that require multiple criteria to be met, (ie `x > 10 && y < 100`), there
should be tests demonstrating that the branch is taken when all critera are met (ie `x=11,y=99`), and that it is not taken when only one is met 
(ie: `x=11, y=200 or x=5,y=99`). If a method deals with ranges of values, (ie `x >= 10`), the boundaries of the ranges should be tested (ie: `x=9, x=10`)

In the following example

**Example**
```java
class SomeClass
{
	public static int someFunction(bool aFlag, int aValue)
	{
		if (aFlag && aValue > 10)
		{
			return 20;
		}
		else if (aValue > 5)
		{
			return 10;
		else
		{
			return 0;
		}
	}
}

class SomeTest
{
	public void someFunction() throws Exception
	{
		assertEquals(10, somefunction(true, 11));
		assertEquals(5, somefunction(false, 11));
		assertEquals(5, somefunction(true, 8));
		assertEquals(5, somefunction(false, 8));
		assertEquals(0, somefunction(false, 4));
	}
}
```

### Test any state transitions

As an extension of testing all branches and inputs. For stateful systems, there should be tests demonstrating that states change under the intended 
circumstances, and that state changes have the intended side effects.
 
### Test unsupported arguments and states throw exceptions

If a system is not intended to perform an action in a given state (ie: a node performing reads during bootstrap), or a method is not intended
to encounter some type of argument (ie: a method that is only designed to work with numeric values > 0), then there should be tests demonstrating
that an appropriate exception is raised (IllegalStateException or IllegalArgumentException, respectively) in that case.

The guava preconditions module makes this straightforward.

#### Reason

Inadvertent misuse of methods and systems cause bugs that are often silent and subtle. Raising exceptions on unintended usage helps
protect against future bugs and reduces developer surprise.

## Dealing with global state

Unfortunately, the project has extensive amounts of global state which makes actually writing robust tests difficult, but not impossible.

Having dependencies on global state is not an excuse to not test something, or to throw a dtest or assertion at it and call it a day.

Structuring code in a way that interacts with global state that can still be deterministically tested just takes a few tweaks

**Example, bad**
```java
class SomeVerbHandler implements IVerbHandler<SomeMessage>
{
	public void doVerb(MessageIn<SomeMessage> msg)
	{
		if (FailureDetector.instance.isAlive(msg.payload.otherNode))
		{
			new StreamPlan(msg.payload.otherNode).requestRanges(someRanges).execute();
		}
		else
		{
			CompactionManager.instance.submitBackground(msg.payload.cfs);
		}
	}
}
```

In this made up example, we're checking global state, and then taking some action against other global state. None of the global state
we're working with is easy to manipulate for tests, so comprehensive tests for this aren't very likely to be written. Even worse, whether
the FailureDetector, streaming, or compaction work properly are out of scope for our purposes. We're concerned with whether SomeVerbHandler
works as expected.

Ideally though, classes won't have dependencies on global state at all, and have everything they need to work passed in as constructor arguments.
This also enables comprehensive testing, and stops the spread of global state. 

This prevents the spread of global state, and also begins to identify and define the internal interfaces that will replace global state.

**Example, better**
```java
class SomeVerbHandler implements IVerbHandler<SomeMessage>
{
	private final IFailureDetector failureDetector;
	private final ICompactionManager compactionManager;
	private final IStreamManager streamManager;

	public SomeVerbHandler(IFailureDetector failureDetector, ICompactionManager compactionManager, IStreamManager streamManager)
	{
		this.failureDetector = failureDetector;
		this.compactionManager = compactionManager;
		this.streamManager = streamManager;
	}

	public void doVerb(MessageIn<SomeMessage> msg)
	{
		if (failureDetector.isAlive(msg.payload.otherNode))
		{
			streamExecutor.submitPlan(new StreamPlan(msg.payload.otherNode).requestRanges(someRanges));
		}
		else
		{
			compactionManager.submitBackground(msg.payload.cfs);
		}
	}
}
```

**Example test**
```java
class SomeVerbTest
{
	class InstrumentedFailureDetector implements IFailureDetector
	{
		boolean alive = false;
		@Override
		public boolean isAlive(InetAddress address)
		{
			return alive;
		}
	}

	class InstrumentedCompactionManager implements ICompactionManager
	{
		boolean submitted = false;
		@Override
		public void submitBackground(ColumnFamilyStore cfs)
		{
			submitted = true;
		}
	}

	class InstrumentedStreamManager implements IStreamManager
	{
		boolean submitted = false;
		@Override
		public void submitPlan(StreamPlan plan)
		{
			submitted = true;
		}
	}

	@Test
	public void liveNode() throws Exception
	{
		InstrumentedFailureDetector failureDetector = new InstrumentedFailureDetector();
		failureDetector.alive = true;
		InstrumentedCompactionManager compactionManager = new InstrumentedCompactionManager();
		InstrumentedStreamManager streamManager = new InstrumentedStreamManager();
		SomeVerbHandler handler = new SomeVerbHandler(failureDetector, compactionManager, streamManager);

		MessageIn<SomeMessage> msg = new MessageIn<>(...);

		assertFalse(streamManager.submitted);
		assertFalse(compactionManager.submitted);

		handler.doVerb(msg);

		assertTrue(streamManager.submitted);
		assertFalse(compactionManager.submitted);
	}

	@Test
	public void deadNode() throws Exception
	{
		InstrumentedFailureDetector failureDetector = new InstrumentedFailureDetector();
		failureDetector.alive = false;
		InstrumentedCompactionManager compactionManager = new InstrumentedCompactionManager();
		InstrumentedStreamManager streamManager = new InstrumentedStreamManager();
		SomeVerbHandler handler = new SomeVerbHandler(failureDetector, compactionManager, streamManager);

		MessageIn<SomeMessage> msg = new MessageIn<>(...);

		assertFalse(streamManager.submitted);
		assertFalse(compactionManager.submitted);

		handler.doVerb(msg);

		assertFalse(streamManager.submitted);
		assertTrue(compactionManager.submitted);
	}
}
```

By abstracting away accesses to global state we can exhaustively test the paths this verb handler can take, and directly confirm that it's taking the correct 
actions. Obviously, this is a simple example, but for classes or functions with more complex logic, this makes comprehensive testing much easier.

Note that the interfaces used here probably shouldn't be the same ones we use for MBeans.

However, in some cases, passing interfaces into the constructor may not be practical. Classes that are instantiated on startup need to be handled with care, since accessing
a singleton may change the initialization order of the database. It may also be a larger change than is warranted for something like a bug fix. In any case, if passing
dependencies into the constructor is not practical, wrapping accesses to global state in protected methods that are overridden for tests will achieve the same thing.


**Example, alternative**
```java
class SomeVerbHandler implements IVerbHandler<SomeMessage>
{ 
	@VisibleForTesting
	protected boolean isAlive(InetAddress addr) { return FailureDetector.instance.isAlive(msg.payload.otherNode); }

	@VisibleForTesting
	protected void streamSomething(InetAddress to) { new StreamPlan(to).requestRanges(someRanges).execute(); }

	@VisibleForTesting
	protected void compactSomething(ColumnFamilyStore cfs ) { CompactionManager.instance.submitBackground(); }

	public void doVerb(MessageIn<SomeMessage> msg)
	{
		if (isAlive(msg.payload.otherNode))
		{
			streamSomething(msg.payload.otherNode);
		}
		else
		{
			compactSomething();
		}
	}
}
```

**Example test**
```java
class SomeVerbTest
{
	static class InstrumentedSomeVerbHandler extends SomeVerbHandler
	{
		public boolean alive = false;
		public boolean streamCalled = false;
		public boolean compactCalled = false;

		@Override
		protected boolean isAlive(InetAddress addr) { return alive; }
		
		@Override
		protected void streamSomething(InetAddress to) { streamCalled = true; }

		@Override
		protected void compactSomething(ColumnFamilyStore cfs ) { compactCalled = true; }
	}

	@Test
	public void liveNode() throws Exception
	{
		InstrumentedSomeVerbHandler handler = new InstrumentedSomeVerbHandler();
		handler.alive = true;
		MessageIn<SomeMessage> msg = new MessageIn<>(...);

		assertFalse(handler.streamCalled);
		assertFalse(handler.compactCalled);

		handler.doVerb(msg);

		assertTrue(handler.streamCalled);
		assertFalse(handler.compactCalled);
	}

	@Test
	public void deadNode() throws Exception
	{
		InstrumentedSomeVerbHandler handler = new InstrumentedSomeVerbHandler();
		handler.alive = false;
		MessageIn<SomeMessage> msg = new MessageIn<>(...);

		assertFalse(handler.streamCalled);
		assertFalse(handler.compactCalled);

		handler.doVerb(msg);

		assertFalse(handler.streamCalled);
		assertTrue(handler.compactCalled);
	}
}
```

## Refactoring Existing Code

If you're working on a section of the project that historically hasn't been well tested, it will likely be more difficult for you to write tests around
whatever you're doing, since the code may not have been written with testing in mind. You do need to add tests around the subject of you're 
jira, and this will probably involve some refactoring, but you're also not expected to completely refactor a huge class to submit a bugfix. 

Basically, you need to be able to verify the behavior you intend to modify before and after your patch. The amount of testing debt you pay back should be 
roughly proportional to the scope of your change. If you're doing a small bugfix, you can get away with refactoring just enough to make the subject of your 
fix testable, even if you start to get into 'testing implementation details' territory'. The goal is incremental improvement, not making things perfect on 
the first iteration. If you're doing something more ambitious though, you may have to do some extra work to sufficiently test your changes.

## Refactoring Untested Code

There are several components that have very little, if any, direct test coverage. We really should try to improve the test coverage of these components.
For people interested in getting involved with the project, adding tests for these is a great way to get familiar with the codebase.

First, get feedback on the subject and scope of your proposed refactor, especially larger ones. The smaller and more narrowly focused your proposed 
refactor is, the easier it will be for you to get it reviewed and committed.

Start with smaller pieces, refactor and test them, and work outwards, iteratively. Preferably in several jiras. Ideally, each patch should add some value
to the project on it's own in terms of test coverage. Patches that are heavy on refactoring, and light on tests are not likely to get committed. People come and go
from projects, and having a many small improvements is better for the project than several unfinished or ongoing refactors that don't add much test coverage.
