package org.calrissian.accumulorecipes.graphstore.tinkerpop;

import com.tinkerpop.blueprints.*;
import org.calrissian.accumulorecipes.graphstore.GraphStore;

public class AccumuloEntityGraph implements Graph{

  protected GraphStore graphStore;

  public AccumuloEntityGraph(GraphStore graphStore) {
    this.graphStore = graphStore;
  }

  @Override
  public Features getFeatures() {
    return null;
  }

  @Override
  public Vertex addVertex(Object o) {
    return null;
  }

  @Override
  public Vertex getVertex(Object o) {
    return null;
  }

  @Override
  public void removeVertex(Vertex vertex) {

  }

  @Override
  public Iterable<Vertex> getVertices() {
    return null;
  }

  @Override
  public Iterable<Vertex> getVertices(String s, Object o) {
    return null;
  }

  @Override
  public Edge addEdge(Object o, Vertex vertex, Vertex vertex2, String s) {
    return null;
  }

  @Override
  public Edge getEdge(Object o) {
    return null;
  }

  @Override
  public void removeEdge(Edge edge) {

  }

  @Override
  public Iterable<Edge> getEdges() {
    return null;
  }

  @Override
  public Iterable<Edge> getEdges(String s, Object o) {
    return null;
  }

  @Override
  public GraphQuery query() {
    return null;
  }

  @Override
  public void shutdown() {

  }
}
