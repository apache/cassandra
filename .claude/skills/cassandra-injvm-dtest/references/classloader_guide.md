# Classloader Deep Dive

Comprehensive guide to understanding and debugging classloader issues in the Cassandra in-JVM dtest framework.

## Classloader Architecture

### The Three ClassLoader Layers

The in-JVM dtest framework uses a three-tier classloader hierarchy:

```
┌─────────────────────────────────────────┐
│     Test ClassLoader (Your Tests)      │
│  - JUnit test code                      │
│  - Test utilities                       │
│  - Direct Cluster/Instance access      │
└─────────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────┐
│      Shared ClassLoader                 │
│  - Primitives (String, Integer, etc.)   │
│  - Config objects (@Shared)             │
│  - API interfaces (ICoordinator, etc.)  │
│  - Serializable contracts               │
└─────────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────┐
│   Instance ClassLoaders (Per Node)      │
│  - DatabaseDescriptor                   │
│  - StorageService                       │
│  - All Cassandra internal classes       │
│  - Logging configuration                │
│  - Each instance gets own copy          │
└─────────────────────────────────────────┘
```

### What Gets Loaded Where

**Shared ClassLoader** (available to all):
- Java primitives: String, Integer, Long, etc.
- Config objects: InstanceConfig, IInstanceConfig
- API interfaces: ICoordinator, IInstance, IInvokableInstance
- ByteBuffer and related classes
- Classes marked with `@Shared` annotation
- Serialization infrastructure

**Instance ClassLoader** (per-instance isolation):
- DatabaseDescriptor (each instance has own static state)
- StorageService (separate singleton per instance)
- All Cassandra core classes (schema, storage, etc.)
- Logging configuration (logback per instance)
- Classes marked with `@Isolated` annotation
- Everything else in org.apache.cassandra.*

### Why This Matters

Each instance needs its own isolated copy of Cassandra's static state:
- DatabaseDescriptor holds configuration → each node needs different config
- StorageService is a singleton → each node needs separate instance
- Schema metadata is static → each node may have different schema versions

## Code Transfer Mechanism

### How Lambdas Cross Boundaries

When you call `runOnInstance(() -> { ... })`:

1. **Serialization** (Test ClassLoader):
   ```
   Lambda → Java Serialization → byte[]
   ```

2. **Transfer** (Across Boundary):
   ```
   byte[] passed to IsolatedExecutor
   ```

3. **Deserialization** (Instance ClassLoader):
   ```
   byte[] → Java Deserialization → Lambda (in instance classloader)
   ```

4. **Execution** (Instance Context):
   ```
   Lambda runs with access to instance's Cassandra classes
   ```

### What Can Be Captured

**Safe to Capture** (automatically serialized):
```java
// Primitives
final int value = 42;
final String name = "test";
final long timestamp = System.currentTimeMillis();

cluster.get(1).runOnInstance(() -> {
    System.out.println(value);    // OK
    System.out.println(name);     // OK
    System.out.println(timestamp); // OK
});
```

**Unsafe to Capture** (serialization will fail):
```java
// Non-serializable objects from test
FileOutputStream fos = new FileOutputStream("test.txt");
AtomicInteger counter = new AtomicInteger(0);
DatabaseDescriptor descriptor = ...;  // From test classloader

cluster.get(1).runOnInstance(() -> {
    fos.write(...);          // FAIL: NotSerializableException
    counter.incrementAndGet(); // FAIL: NotSerializableException
    descriptor.get...();     // FAIL: Even if serializable, wrong classloader
});
```

## Common Issues and Solutions

### Issue 1: ClassCastException

**Symptom:**
```
java.lang.ClassCastException: org.apache.cassandra.dht.Murmur3Partitioner$LongToken cannot be cast to org.apache.cassandra.dht.Token
```

**Cause:**
Object created in test classloader, trying to use in instance classloader. Even though the classes have the same name, they're loaded by different classloaders and are considered different types.

**Wrong:**
```java
// Create token in test classloader
Token token = new Murmur3Partitioner.LongToken(0);

// Try to use in instance
cluster.get(1).runOnInstance(() -> {
    DatabaseDescriptor.setPartitionerUnsafe(token); // FAIL: ClassCastException
});
```

**Right:**
```java
// Create token inside instance classloader
cluster.get(1).runOnInstance(() -> {
    Token token = new Murmur3Partitioner.LongToken(0);
    DatabaseDescriptor.setPartitionerUnsafe(token); // OK
});
```

**Why This Happens:**
```
Test CL: Token.class  → Object ID 0x1234
Instance CL: Token.class → Object ID 0x5678

0x1234 cannot be cast to 0x5678 even if identical bytecode
```

### Issue 2: NotSerializableException

**Symptom:**
```
java.io.NotSerializableException: MyCustomClass
```

**Cause:**
Lambda captured non-serializable object from test scope.

**Wrong:**
```java
List<String> names = new ArrayList<>();  // ArrayList is Serializable
Thread thread = new Thread(() -> {});    // Thread is NOT Serializable

cluster.get(1).runOnInstance(() -> {
    names.add("test");  // OK - ArrayList is Serializable
    thread.start();     // FAIL - Thread is not Serializable
});
```

**Right:**
```java
// Only capture serializable primitives
final String threadName = "test-thread";

cluster.get(1).runOnInstance(() -> {
    Thread thread = new Thread(() -> {}, threadName);  // Create inside
    thread.start();  // OK
});
```

### Issue 3: Static State Not Shared

**Symptom:**
Static fields modified in one instance don't affect others.

**Cause:**
Each instance has separate copy of static state.

**Wrong Assumption:**
```java
// Set on instance 1
cluster.get(1).runOnInstance(() -> {
    DatabaseDescriptor.setPartitioner(...);
});

// Expect to see on instance 2
cluster.get(2).runOnInstance(() -> {
    // DatabaseDescriptor is DIFFERENT instance here
    Partitioner p = DatabaseDescriptor.getPartitioner();  // Won't see change from node 1
});
```

**Right Approach:**
```java
// Configure each instance separately
cluster.stream().forEach(instance -> {
    instance.runOnInstance(() -> {
        DatabaseDescriptor.setPartitioner(...);
    });
});
```

### Issue 4: ClassNotFoundException

**Symptom:**
```
java.lang.ClassNotFoundException: com.mycompany.TestUtils
```

**Cause:**
Class from test classpath not available in instance classloader.

**Wrong:**
```java
// TestUtils only in test classpath
import com.mycompany.TestUtils;

cluster.get(1).runOnInstance(() -> {
    TestUtils.doSomething();  // FAIL: ClassNotFoundException
});
```

**Right:**
```java
// Either:
// 1. Move TestUtils to Cassandra dependencies
// 2. Mark TestUtils as @Shared
// 3. Use only Cassandra classes inside runOnInstance

cluster.get(1).runOnInstance(() -> {
    // Use only Cassandra classes
    StorageService.instance.method();  // OK
});
```

### Issue 5: Lambda Capture Confusion

**Symptom:**
Unexpected behavior or null values inside lambda.

**Cause:**
Captured variables must be effectively final.

**Wrong:**
```java
int counter = 0;

for (int i = 0; i < 3; i++) {
    cluster.get(i + 1).runOnInstance(() -> {
        System.out.println(counter);  // FAIL: counter must be final
    });
    counter++;
}
```

**Right:**
```java
for (int i = 0; i < 3; i++) {
    final int nodeId = i + 1;  // Effectively final
    cluster.get(nodeId).runOnInstance(() -> {
        System.out.println("Node: " + nodeId);  // OK
    });
}
```

## Debugging Techniques

### Technique 1: Identify ClassLoader

Print which classloader loaded a class:

```java
cluster.get(1).runOnInstance(() -> {
    System.out.println("Current thread CL: " +
        Thread.currentThread().getContextClassLoader());

    System.out.println("Token CL: " +
        Token.class.getClassLoader());

    System.out.println("DatabaseDescriptor CL: " +
        DatabaseDescriptor.class.getClassLoader());
});
```

Expected output:
```
Current thread CL: IsolatedClassLoader@abc123
Token CL: IsolatedClassLoader@abc123
DatabaseDescriptor CL: IsolatedClassLoader@abc123
```

### Technique 2: Verify Serialization

Test if object can be serialized before using in lambda:

```java
public static boolean isSerializable(Object obj) {
    try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        return true;
    } catch (NotSerializableException e) {
        System.out.println("NOT serializable: " + e.getMessage());
        return false;
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
}

// Usage
MyObject obj = new MyObject();
if (isSerializable(obj)) {
    // Safe to capture in lambda
    final MyObject capturedObj = obj;
    cluster.get(1).runOnInstance(() -> {
        // Use capturedObj
    });
} else {
    // Must create inside runOnInstance
}
```

### Technique 3: Explicit Transfer

Use IsolatedExecutor.transfer() to explicitly move objects:

```java
import org.apache.cassandra.distributed.impl.IsolatedExecutor;

// Create in test classloader
MySerializableObject obj = new MySerializableObject();

// Explicitly transfer to instance classloader
cluster.get(1).runOnInstance(() -> {
    MySerializableObject transferred = (MySerializableObject)
        IsolatedExecutor.transferAdhoc(obj, Thread.currentThread().getContextClassLoader());

    // Now 'transferred' is in correct classloader
    useObject(transferred);
});
```

### Technique 4: Inspect Lambda Captures

Use reflection to see what a lambda captured:

```java
import java.lang.reflect.Field;

Consumer<Object> lambda = (obj) -> {
    System.out.println(obj);
};

// Inspect captured fields
for (Field field : lambda.getClass().getDeclaredFields()) {
    field.setAccessible(true);
    System.out.println("Captured: " + field.getName() +
                      " = " + field.get(lambda));
}
```

### Technique 5: Compare Class Identity

Check if two class references are actually the same:

```java
Class<?> testToken = Token.class;  // From test classloader

cluster.get(1).runOnInstance(() -> {
    Class<?> instanceToken = Token.class;  // From instance classloader

    // These will be DIFFERENT
    System.out.println("Test CL Token: " + System.identityHashCode(testToken));
    System.out.println("Instance CL Token: " + System.identityHashCode(instanceToken));
    System.out.println("Same class? " + (testToken == instanceToken));  // false!
});
```

## Annotations: @Shared and @Isolated

### @Shared Annotation

Mark classes to be loaded in shared classloader:

```java
import org.apache.cassandra.utils.Shared;

@Shared
public class MySharedData implements Serializable {
    private String value;

    // Can be used across classloader boundaries
    public MySharedData(String value) {
        this.value = value;
    }
}
```

**Use @Shared for:**
- Config/DTO classes
- Data transfer objects
- Immutable value classes
- Things that need to cross boundaries

### @Isolated Annotation

Mark classes to be loaded per-instance:

```java
import org.apache.cassandra.utils.Isolated;

@Isolated
public class MyInstanceState {
    private static MyInstanceState instance;

    // Each Cassandra instance gets own copy
    public static MyInstanceState getInstance() {
        if (instance == null)
            instance = new MyInstanceState();
        return instance;
    }
}
```

**Use @Isolated for:**
- Stateful singletons
- Classes with static state
- Instance-specific configuration

## Best Practices

### 1. Minimize Lambda Captures

**Bad:**
```java
List<String> results = new ArrayList<>();
Map<String, Integer> counts = new HashMap<>();
AtomicInteger total = new AtomicInteger();

cluster.get(1).runOnInstance(() -> {
    // Captures 3 objects
    results.add("test");
    counts.put("key", 1);
    total.incrementAndGet();
});
```

**Good:**
```java
cluster.get(1).runOnInstance(() -> {
    // Create everything inside
    List<String> results = new ArrayList<>();
    results.add("test");
    // Process locally
});
```

### 2. Use Return Values

**Bad:**
```java
AtomicInteger result = new AtomicInteger();

cluster.get(1).runOnInstance(() -> {
    int count = countSomething();
    result.set(count);  // Won't work - different classloader
});
```

**Good:**
```java
int result = cluster.get(1).callOnInstance(() -> {
    return countSomething();  // Return primitive
});
```

### 3. Create Objects in Target ClassLoader

**Bad:**
```java
Token token = new Murmur3Partitioner.LongToken(0);

cluster.get(1).runOnInstance(() -> {
    useToken(token);  // ClassCastException
});
```

**Good:**
```java
final long tokenValue = 0;

cluster.get(1).runOnInstance(() -> {
    Token token = new Murmur3Partitioner.LongToken(tokenValue);
    useToken(token);  // OK
});
```

### 4. Use Primitives for Communication

**Bad:**
```java
MyComplexObject obj = new MyComplexObject(...);

cluster.get(1).runOnInstance(() -> {
    process(obj);  // May fail serialization
});
```

**Good:**
```java
final int id = obj.getId();
final String name = obj.getName();

cluster.get(1).runOnInstance(() -> {
    MyComplexObject obj = new MyComplexObject(id, name);
    process(obj);  // OK - created in instance classloader
});
```

### 5. Coordinate via CQL

**Bad:**
```java
// Try to share objects between instances
Object sharedState = cluster.get(1).callOnInstance(() -> {
    return getState();
});

cluster.get(2).runOnInstance(() -> {
    setState(sharedState);  // ClassCastException
});
```

**Good:**
```java
// Share via CQL
cluster.coordinator(1).execute(
    "INSERT INTO shared.state (id, value) VALUES (?, ?)",
    ConsistencyLevel.QUORUM, 1, "state-value");

// Read on other instance
Object[][] rows = cluster.coordinator(2).execute(
    "SELECT value FROM shared.state WHERE id = ?",
    ConsistencyLevel.QUORUM, 1);
```

## Advanced: Understanding IsolatedExecutor

### Serialization Process

```java
// In test classloader
SerializableCallable<String> lambda = () -> "Hello";

// IsolatedExecutor.transfer():
byte[] serialized = serializeOneObject(lambda);  // Test CL → bytes

// Transfer to instance classloader
Object deserialized = deserializeOneObject(serialized);  // bytes → Instance CL

// Now deserialized is in instance classloader
```

### Custom Transfer

For complex scenarios:

```java
// Serialize in test classloader
ByteArrayOutputStream baos = new ByteArrayOutputStream();
ObjectOutputStream oos = new ObjectOutputStream(baos);
oos.writeObject(myObject);
byte[] bytes = baos.toByteArray();

// Transfer and deserialize in instance classloader
cluster.get(1).runOnInstance(() -> {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais) {
        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            // Force resolution in instance classloader
            return Class.forName(desc.getName(), false,
                               Thread.currentThread().getContextClassLoader());
        }
    };

    Object obj = ois.readObject();
    // obj is now in instance classloader
});
```

## Troubleshooting Checklist

When encountering classloader issues:

1. **Check what you're capturing**
   - Is it a primitive? → OK
   - Is it Serializable? → Maybe OK
   - Is it a Cassandra class? → Create inside runOnInstance

2. **Verify serialization**
   - Use isSerializable() helper
   - Check for non-serializable fields

3. **Identify the classloader**
   - Print classloader info
   - Compare class identity

4. **Simplify the lambda**
   - Remove captures one by one
   - Create objects inside

5. **Use primitives**
   - Extract primitive values
   - Pass only primitives

6. **Return, don't modify**
   - Return results from callOnInstance
   - Don't try to modify captured objects
