package org.calrissian.accumulorecipes.commons.collect;

import com.google.common.collect.Iterables;
import org.calrissian.mango.domain.BaseEvent;
import org.calrissian.mango.domain.Event;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static java.lang.System.currentTimeMillis;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;

public class StoreEntryMergeJoinIterableTest {

  @Test
  public void test() {

    Event entry1 = new BaseEvent(randomUUID().toString(), currentTimeMillis() - 5000);
    Event entry2 = new BaseEvent(randomUUID().toString(), currentTimeMillis());
    Event entry3 = new BaseEvent(randomUUID().toString(), currentTimeMillis() - 30);

    List<Iterable<Event>> entryIter = new LinkedList<Iterable<Event>>();
    entryIter.add(Arrays.asList(new Event[]{entry1}));
    entryIter.add(Arrays.asList(new Event[]{entry2}));
    entryIter.add(Arrays.asList(new Event[]{entry3}));

    EventMergeJoinIterable iterable = new EventMergeJoinIterable(entryIter);

    assertEquals(3, Iterables.size(iterable));
    assertEquals(entry2, Iterables.get(iterable, 0));
    assertEquals(entry3, Iterables.get(iterable, 1));
    assertEquals(entry1, Iterables.get(iterable, 2));

  }
}
