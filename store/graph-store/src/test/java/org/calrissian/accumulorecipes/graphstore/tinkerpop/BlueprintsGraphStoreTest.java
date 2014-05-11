package org.calrissian.accumulorecipes.graphstore.tinkerpop;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.graphstore.impl.AccumuloEntityGraphStore;
import org.junit.Ignore;

import java.lang.reflect.Method;
import java.util.Collections;

@Ignore
public class BlueprintsGraphStoreTest extends GraphTest {

  public void testGraphTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new GraphTestSuite(this));
    printTestPerformance("GraphTestSuite", this.stopWatch());
  }

  @Override
  public Graph generateGraph() {
    Instance instance = new MockInstance();

    try {
      Connector connector = instance.getConnector("root", "".getBytes());

      return new BlueprintsGraphStore(new AccumuloEntityGraphStore(connector), Collections.singleton("vertex"),
              Collections.singleton("edge"), new Auths());

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Graph generateGraph(String s) {
    Instance instance = new MockInstance();

    try {
      Connector connector = instance.getConnector("root", "".getBytes());

      return new BlueprintsGraphStore(new AccumuloEntityGraphStore(connector), Collections.singleton("vertex"),
              Collections.singleton("edge"), new Auths());

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void doTestSuite(TestSuite testSuite) throws Exception {
    for (Method method : testSuite.getClass().getDeclaredMethods()) {
      if (method.getName().startsWith("test")) {
        System.out.println("Testing " + method.getName() + "...");
        method.invoke(testSuite);
      }
    }
  }

}
