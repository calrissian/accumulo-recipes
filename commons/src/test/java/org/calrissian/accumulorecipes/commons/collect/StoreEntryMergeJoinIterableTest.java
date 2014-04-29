package org.calrissian.accumulorecipes.commons.collect;

import com.google.common.collect.Iterables;
import org.calrissian.accumulorecipes.commons.collect.StoreEntryMergeJoinIterable;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
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

        StoreEntry entry1 = new StoreEntry(randomUUID().toString(), currentTimeMillis() - 5000);
        StoreEntry entry2 = new StoreEntry(randomUUID().toString(), currentTimeMillis());
        StoreEntry entry3 = new StoreEntry(randomUUID().toString(), currentTimeMillis() - 30);

        List<Iterable<StoreEntry>> entryIter = new LinkedList<Iterable<StoreEntry>>();
        entryIter.add(Arrays.asList(new StoreEntry[] { entry1 }));
        entryIter.add(Arrays.asList(new StoreEntry[] { entry2 }));
        entryIter.add(Arrays.asList(new StoreEntry[] { entry3 }));

        StoreEntryMergeJoinIterable iterable = new StoreEntryMergeJoinIterable(entryIter);

        assertEquals(3, Iterables.size(iterable));
        assertEquals(entry2, Iterables.get(iterable, 0));
        assertEquals(entry3, Iterables.get(iterable, 1));
        assertEquals(entry1, Iterables.get(iterable, 2));

    }
}
