package org.calrissian.accumulorecipes.commons.iterators.support;

import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NodeToJexlTest {

  private NodeToJexl nodeToJexl = new NodeToJexl();

  @Test
  public void testSimpleEquals_AndNode() {
    String jexl = nodeToJexl.transform(new QueryBuilder().and().eq("hello", "goodbye").eq("key1", true).end().build());
    assertEquals("((hello == 'string\u0001goodbye') and (key1 == 'boolean\u00011'))", jexl) ;
  }

  @Test
  public void testSimpleEquals_OrNode() {
    String jexl = nodeToJexl.transform(new QueryBuilder().or().eq("hello", "goodbye").eq("key1", true).end().build());
    assertEquals("((hello == 'string\u0001goodbye') or (key1 == 'boolean\u00011'))", jexl) ;
  }

  @Test
  public void testGreaterThan() {
    String jexl = nodeToJexl.transform(new QueryBuilder().greaterThan("hello", "goodbye").build());
    assertEquals("((hello > 'string\u0001goodbye'))", jexl) ;
  }

  @Test
  public void testLessThan() {
    String jexl = nodeToJexl.transform(new QueryBuilder().lessThan("hello", "goodbye").build());
    assertEquals("((hello < 'string\u0001goodbye'))", jexl) ;
  }


  @Test
  public void testGreaterThanEquals() {
    String jexl = nodeToJexl.transform(new QueryBuilder().greaterThanEq("hello", "goodbye").build());
    assertEquals("((hello >= 'string\u0001goodbye'))", jexl) ;
  }

  @Test
  public void testLessThanEquals() {
    String jexl = nodeToJexl.transform(new QueryBuilder().lessThanEq("hello", "goodbye").build());
    assertEquals("((hello <= 'string\u0001goodbye'))", jexl) ;
  }

  @Test
  public void testNotEquals() {
    String jexl = nodeToJexl.transform(new QueryBuilder().notEq("hello", "goodbye").build());
    assertEquals("((hello != 'string\u0001goodbye'))", jexl) ;
  }

  @Test
  public void testHas() {
    String jexl = nodeToJexl.transform(new QueryBuilder().has("hello").build());
    assertEquals("((hello >= '\u0000'))", jexl) ;
  }

  @Test
  public void testHasNot() {
    String jexl = nodeToJexl.transform(new QueryBuilder().hasNot("hello").build());
    assertEquals("(!(hello >= '\u0000'))", jexl) ;
  }

}

