package org.calrissian.accumulorecipes.test;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Map;

public class AccumuloTestUtils {

  private AccumuloTestUtils() {}

  public static void dumpTable(Connector connector, String table, Authorizations auths) throws TableNotFoundException {
    Scanner scanner = connector.createScanner(table, auths);
    for(Map.Entry<Key,Value> entry : scanner) {
      System.out.println("ENTRY: " + entry);
    }
  }

  public static void dumpTable(Connector connector, String table) throws TableNotFoundException {
    dumpTable(connector, table, new Authorizations());
  }
}
