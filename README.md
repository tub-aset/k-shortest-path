# k-shortest-path
Yen's Algorithm for K-Shortest-Path for Apache Tinkerpop
Implementation for apache/tinkerpop

## Usage

Hint: A found path and its cost are returned as tuple using `Map.Entry`


The API heavily uses builder pattern, including defaults like tinkerpop. The following examples show almost all possible options available.

First, an instance is generated (`1` and `5` are source and target id):

```java
Graph graph = TinkerFactory.createModern();
YenAlgorithm algorithm = YenAlgorithm.build(graph.traversal(), 1, 5).build();
```

To get a sorted list of `2` shortest paths including path cost:

```java
List<Map.Entry<Path, Double>> list = algorithm.list(2);
Map.Entry<Path, Double> first = list.get(0);
Map.Entry<Path, Double> second = list.get(1);
```

Alternatively, `YenAlgorithm` implements `Iterable<Map.Entry<Path, Double>>` to provide an iterator:

```java
Iterator<Map.Entry<Path, Double>> iterator = algorithm.iterator();
Map.Entry<Path, Double> first = iterator.next();
Map.Entry<Path, Double> second = iterator.next();
```

To use a certain `ShortestPath` configuration , `YenAlgorithm.Builder` provides corresponding methods and values.
See https://tinkerpop.apache.org/docs/3.4.3/reference/#shortestpath-step

`ShortestPath.distance` (`Traversal` or `String`)

```java
YenAlgorithm algorithm = YenAlgorithm.build(graph.traversal(), 1, 5)
	.distance("weight")
	.build();
```

```java
YenAlgorithm algorithm = YenAlgorithm.build(graph.traversal(), 1, 5)
	.distance(__.values("weight").math("1 / _"))
	.build();
```

`ShortestPath.maxDistance` (`Number`)

```java
YenAlgorithm algorithm = YenAlgorithm.build(graph.traversal(), 1, 5)
	.maxDistance(2d)
	.build();
```

`ShortestPath.edges` (`Traversal` or `Direction`)

```java
YenAlgorithm algorithm = YenAlgorithm.build(graph.traversal(), 1, 5)
	.edges(Direction.OUT)
	.build();
```

`ShortestPath.includeEdges` (`Boolean`)

```java
YenAlgorithm algorithm = YenAlgorithm.build(graph.traversal(), 1, 5)
	.includeEdges(true)
	.build();
```

Hint: if `includeEdges` is true, only the edges of the known path are disabled, otherwise all edges between vertices of the path are disabled.

Since `ShortestPath` needs a `GraphComputer` to process, `YenAlgorithm.Builder` also provides methods to configure:

By default, `YenAlgorithm` uses `traversal.withComputer()` to process `ShortestPath`.

To disable the generation of a new computer (e.g. traversal already has one):

```java
YenAlgorithm algorithm = YenAlgorithm.build(graph.traversal(), 1, 5)
	.computer(false)
	.build();
```


## Current Implementation Features

* based on https://gist.github.com/ALenfant/5491853
* `numK` does not have to be predefined (`Iterable`)
* distances/costs are calculated via `Double`
* edges are not removed but disabled via `Double.POSITIVE_INFINITY` (using `ShortestPath.distance`)