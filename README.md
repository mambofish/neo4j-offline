# Neo4j-Offline

A collection of utilities to make it easier to work with different versions of Neo4j in offline mode.

## BatchInserterWrapper
The BatchInserterWrapper is a version-agnostic replacement for Neo4j's own BatchInserter. It handles all the 
API changes that have occurred over time in the underlying Neo4j classes, and can be used with any version of 
Neo4j from 2.0 onwards.

### Maven
```yaml
<dependency>
    <groupId>org.mambofish</groupId>
    <artifactId>neo4j-offline</artifactId>
    <version>1.0</version>
</dependency>
```

### Usage
```
BatchInserterWrapper wrapper = BatchInserterWrapper.connect(Paths.get({path_to_graph.db}).toFile());

... do stuff

wrapper.disconnect();

```

The `wrapper.disconnect()` method is important! Although Neo4j's BatchInserter is not thread-safe, there _are_ ways you can safely use it from multiple threads to gain some performance advantage. For example, if you organise it properly, you can have one thread creating nodes, and another one creating relationships. In this scenario, each thread would obtain a reference to the batch inserter using `connect(...)`, and then call `disconnect()` when it has finished. The BatchInserterWrapper keeps track of all connection requests and when all the threads have disconnected it will shutdown, create any indexes as required, and finally make the graph usable.

### API
Please note: the BatchInserterWrapper API methods refer to Neo4j relationships as `edges`.








