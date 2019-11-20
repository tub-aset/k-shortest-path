package de.tuberlin.aset.kshortestpath;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPath;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class YenAlgorithm implements Iterable<Entry<Path, Double>> {

	private final GraphTraversalSource traversal;
	private final Object sourceId;
	private final Object targetId;
	private final Object computer;
	private final Object edges;
	private final Object distance;
	private final boolean includeEdges;
	private final Number maxDistance;
	private final PropertyKeyFactory propertyKeyFactory;

	public YenAlgorithm(Builder builder) {
		this.traversal = builder.traversal;
		this.sourceId = builder.sourceId;
		this.targetId = builder.targetId;
		this.computer = builder.computer;
		this.edges = builder.edges;
		this.distance = builder.distance;
		this.includeEdges = builder.includeEdges;
		this.maxDistance = builder.maxDistance;

		this.propertyKeyFactory = builder.propertyKeyFactory != null ? builder.propertyKeyFactory
				: new DefaultPropertyKeyFactory(UUID.randomUUID().toString());
	}

	@Override
	public ListIterator<Entry<Path, Double>> iterator() {
		return new ListIterator<Entry<Path, Double>>() {

			private int k = 0;
			private boolean finished = false;
			private List<PathInformation> A = new ArrayList<>();
			private PriorityQueue<PathInformation> B = new PriorityQueue<>();

			@Override
			public boolean hasNext() {
				if (finished && k == A.size()) {
					return false;
				}
				if (k + 1 == A.size()) {
					return true;
				}
				return nextPath() != null;
			}

			@Override
			public Entry<Path, Double> next() {
				if (finished && k == A.size()) {
					throw new NoSuchElementException();
				}
				PathInformation pathInformation = k + 1 <= A.size() ? A.get(k) : nextPath();
				if (pathInformation != null) {
					k++;
					return pathInformation.entry;
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public boolean hasPrevious() {
				return k > 0;
			}

			@Override
			public Entry<Path, Double> previous() {
				if (k == 0) {
					throw new NoSuchElementException();
				}
				PathInformation pathInformation = A.get(--k);
				return pathInformation.entry;
			}

			@Override
			public int nextIndex() {
				return k;
			}

			@Override
			public int previousIndex() {
				return k - 1;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("remove");
			}

			@Override
			public void set(Entry<Path, Double> e) {
				throw new UnsupportedOperationException("set");
			}

			@Override
			public void add(Entry<Path, Double> e) {
				throw new UnsupportedOperationException("add");
			}

			private PathInformation nextPath() {
				if (k == 0) {
					Optional<Path> optional = shortestPath(sourceId, false);
					if (optional.isPresent()) {
						Path path = optional.get();
						double cost = pathCost(path);
						PathInformation pathInformation = new PathInformation(path, cost);
						A.add(pathInformation);
						return pathInformation;
					}
					finished = true;
					return null;
				}
				PathInformation previousPathInformation = A.get(k - 1);
				// The spur node ranges from the first node to the next to last node in the
				// shortest path
				for (int i = 0; i < previousPathInformation.path.size() - 1; i += (includeEdges ? 2 : 1)) {
					// Spur node is retrieved from the previous k-shortest path, k - 1
					Vertex spurNode = previousPathInformation.path.get(i);
					// The sequence of nodes from the source to the spur node of the previous
					// k-shortest path
					Path rootPath = subPath(previousPathInformation.path, i);

					for (PathInformation pathInformation : A) {
						if (pathInformation.path.size() - (includeEdges ? 2 : 1) > i
								&& rootPath.equals(subPath(pathInformation.path, i))) {

							// Remove the links that are part of the previous shortest paths which share the
							// same root path

							GraphTraversal<?, Edge> edgesTraversal;
							if (includeEdges) {
								Edge referenceEdge = pathInformation.path.get(i + 1);
								edgesTraversal = traversal.E(referenceEdge.id());
							} else {
								Vertex referenceVertex = pathInformation.path.get(i + 1);
								edgesTraversal = edgesTraversal(spurNode.id(), referenceVertex.id());
							}
							if (edgesTraversal.asAdmin().clone().has(propertyKeyFactory.disableEdgeKey()).hasNext()) {
								continue; // edge already disabled
							}
							disableEdges(edgesTraversal);
						}
					}

					// Calculate the spur path from the spur node to the sink

					Optional<Path> spurPathOptional = shortestPath(spurNode.id(), true);

					if (spurPathOptional.isPresent()) {
						Path spurPath = spurPathOptional.get();
						// Entire path is made up of the root path and spur path
						Path totalPath = mergePaths(rootPath, spurPath);
						double totalPathCost = pathCost(totalPath);
						// Add the potential k-shortest path to the heap
						B.add(new PathInformation(totalPath, totalPathCost));
					}

					// Add back the edges that were removed from the graph
					enableEdges();
				}
				// Sort the potential k-shortest paths by cost
				// B is already sorted
				// Add the lowest cost path becomes the k-shortest path.
				while (true) {
					PathInformation pathInformation = B.poll();
					if (pathInformation == null) {
						finished = true;
						return null;
					}
					if (!A.contains(pathInformation)) {
						// We found a new path to add
						A.add(pathInformation);
						break;
					}
				}
				PathInformation pathInformation = A.get(k);
				return pathInformation;
			}
		};
	}

	public List<Entry<Path, Double>> list(int numK) {
		List<Entry<Path, Double>> result = new ArrayList<>();
		Iterator<Entry<Path, Double>> iterator = this.iterator();
		for (int i = 0; i < numK; i++) {
			if (iterator.hasNext()) {
				result.add(iterator.next());
			} else {
				break;
			}
		}
		return result;
	}

	private void disableEdges(GraphTraversal<?, Edge> edgesTraversal) {
		edgesTraversal.property(propertyKeyFactory.disableEdgeKey(), Double.POSITIVE_INFINITY).iterate();
	}

	private void enableEdges() {
		traversal.E().has(propertyKeyFactory.disableEdgeKey()).properties(propertyKeyFactory.disableEdgeKey()).drop()
				.iterate();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Optional<Path> shortestPath(Object sourceId, boolean disabledEdges) {
		GraphTraversal<Vertex, Path> traversal = computerTraversal().V(sourceId).shortestPath() //
				.with(ShortestPath.target, __.hasId(targetId)) //
				.with(ShortestPath.includeEdges, includeEdges); //
		if (distance != null) {
			if (disabledEdges) {
				traversal = traversal.with(ShortestPath.distance,
						__.coalesce(__.values(propertyKeyFactory.disableEdgeKey()), distanceTraversal()));
			} else {
				traversal = traversal.with(ShortestPath.distance, distance);
			}
		} else if (disabledEdges) {
			traversal = traversal.with(ShortestPath.distance,
					__.coalesce(__.values(propertyKeyFactory.disableEdgeKey()), __.constant(1)));
		}
		if (this.edges != null) {
			if (this.edges instanceof Traversal) {
				Traversal edges = ((Traversal) this.edges).asAdmin().clone();
				traversal = traversal.with(ShortestPath.edges, edges);
			} else {
				traversal = traversal.with(ShortestPath.edges, this.edges);
			}
		}
		if (maxDistance != null) {
			traversal = traversal.with(ShortestPath.maxDistance, maxDistance);
		}
		return traversal.tryNext();
	}

	@SuppressWarnings("unchecked")
	private GraphTraversalSource computerTraversal() {
		if (computer == null) {
			return traversal.withComputer();
		} else {
			if (computer instanceof Boolean) {
				return (boolean) computer ? traversal.withComputer() : traversal;
			} else if (computer instanceof Computer) {
				return traversal.withComputer((Computer) computer);
			} else if (computer instanceof Class) {
				return traversal.withComputer((Class<? extends GraphComputer>) computer);
			} else {
				throw new IllegalStateException("unknown computer");
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private double pathCost(Path path) {
		if (distance == null) {
			if (includeEdges) {
				return (path.size() - 1) / 2;
			} else {
				return path.size() - 1;
			}
		} else {
			if (includeEdges) {
				List<Object> edgeIds = new ArrayList<>();
				for (int i = 1; i < path.size() - 1; i += 2) {
					Edge referenceEdge = path.get(i);
					edgeIds.add(referenceEdge.id());
				}
				GraphTraversal<Edge, Edge> edgesTraversal = traversal.E(edgeIds.toArray());
				GraphTraversal valuesTraversal = edgesTraversal.map(distanceTraversal());
				return (double) valuesTraversal.sum().next();
			} else {
				double cost = 0d;
				for (int i = 0; i < path.size() - 1; i++) {
					Vertex from = path.get(i);
					Vertex to = path.get(i + 1);
					GraphTraversal<?, Edge> edgeTraversal = edgesTraversal(from.id(), to.id());
					GraphTraversal valueTraversal = edgeTraversal.map(distanceTraversal());
					Object value = valueTraversal.min().next();
					cost += ((Number) value).doubleValue();
				}
				return cost;
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private Traversal distanceTraversal() {
		if (distance instanceof Traversal) {
			return ((Traversal) distance).asAdmin().clone();
		} else if (distance instanceof String) {
			return __.values((String) distance);
		} else {
			throw new IllegalStateException("unknown distance");
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private GraphTraversal<?, Edge> edgesTraversal(Object fromId, Object toId) {
		if (edges == null) {
			return this.traversal.V(fromId).toE(Direction.BOTH).filter(__.toV(Direction.BOTH).hasId(toId));
		} else {
			if (edges instanceof Direction) {
				Direction direction = (Direction) edges;
				return this.traversal.V(fromId).toE(direction).filter(__.toV(direction.opposite()).hasId(toId));
			} else if (edges instanceof Traversal) {
				return (GraphTraversal) __.sideEffect(((Traversal) edges).asAdmin().clone())
						.filter(__.and(__.toV(Direction.BOTH).hasId(fromId), __.toV(Direction.BOTH).hasId(toId)));
			} else {
				throw new IllegalStateException("unknown edges");
			}
		}
	}

	private static Path subPath(Path path, int toIndex) {
		Path subPath = MutablePath.make();
		for (int i = 0; i < toIndex; i++) {
			Object object = path.get(i);
			Set<String> labels = path.labels().get(i);
			subPath.extend(object, labels);
		}
		return subPath;
	}

	private static Path mergePaths(Path startPath, Path endPath) {
		Path path = MutablePath.make();
		for (int i = 0; i < startPath.size(); i++) {
			Object object = startPath.get(i);
			Set<String> labels = startPath.labels().get(i);
			path.extend(object, labels);
		}
		for (int i = 0; i < endPath.size(); i++) {
			Object object = endPath.get(i);
			Set<String> labels = endPath.labels().get(i);
			path.extend(object, labels);
		}
		return path;
	}

	private static class PathInformation implements Comparable<PathInformation> {
		private Path path;
		private double cost;
		private Map.Entry<Path, Double> entry;

		private PathInformation(Path path, double cost) {
			this.path = path;
			this.cost = cost;
			this.entry = new AbstractMap.SimpleImmutableEntry<>(path, cost);
		}

		@Override
		public int hashCode() {
			return Objects.hash(path);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PathInformation other = (PathInformation) obj;
			return Objects.equals(path, other.path);
		}

		@Override
		public int compareTo(PathInformation o) {
			return Double.compare(cost, o.cost);
		}
	}

	public static final Builder build(GraphTraversalSource traversal, Object sourceId, Object targetId) {
		return new Builder(traversal, sourceId, targetId);
	}

	public static final class Builder {

		private GraphTraversalSource traversal;
		private Object sourceId;
		private Object targetId;

		private Object computer = true;
		private Object edges;
		private Object distance;
		private boolean includeEdges = false;
		private Number maxDistance;

		private PropertyKeyFactory propertyKeyFactory;

		private Builder(GraphTraversalSource traversal, Object sourceId, Object targetId) {
			this.traversal = traversal;
			this.sourceId = sourceId;
			this.targetId = targetId;
		}

		public Builder computer(Boolean computer) {
			this.computer = computer;
			return this;
		}

		public Builder computer(Computer computer) {
			this.computer = computer;
			return this;
		}

		public Builder computer(Class<? extends GraphComputer> graphComputerClass) {
			this.computer = graphComputerClass;
			return this;
		}

		public Builder edges(Direction direction) {
			this.edges = direction;
			return this;
		}

		@SuppressWarnings("rawtypes")
		public Builder edges(Traversal traversal) {
			this.edges = traversal.asAdmin().clone();
			return this;
		}

		public Builder distance(String propertyKey) {
			this.distance = propertyKey;
			return this;
		}

		@SuppressWarnings("rawtypes")
		public Builder distance(Traversal distance) {
			this.distance = distance.asAdmin().clone();
			return this;
		}

		public Builder includeEdges(boolean includeEdges) {
			this.includeEdges = includeEdges;
			return this;
		}

		public Builder maxDistance(Number maxDistance) {
			this.maxDistance = maxDistance;
			return this;
		}

		public Builder propertyKeyFactory(PropertyKeyFactory propertyKeyFactory) {
			this.propertyKeyFactory = propertyKeyFactory;
			return this;
		}

		public YenAlgorithm create() {
			return new YenAlgorithm(this);
		}

	}

	public static interface PropertyKeyFactory {

		String disableEdgeKey();

	}

	public static class DefaultPropertyKeyFactory implements PropertyKeyFactory {

		private static final String SEARCH_WEIGHT_PROPERTY_KEY = "disable_edge";

		private static final String SPLIT = "_";

		private String disableEdgeKey;

		public DefaultPropertyKeyFactory(String propertyPrefix) {
			StringBuilder sb = new StringBuilder();
			sb.append(propertyPrefix);
			sb.append(SPLIT);
			sb.append(SEARCH_WEIGHT_PROPERTY_KEY);
			this.disableEdgeKey = sb.toString();
		}

		@Override
		public String disableEdgeKey() {
			return disableEdgeKey;
		}

	}

}
