package org.calrissian.accumlorecipes.changelog.support;

import org.calrissian.commons.domain.Tuple;

import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple> {


    @Override
    public int compare(Tuple tuple, Tuple tuple1) {
        try {
            String tupleString = Utils.tupleToString(tuple);
            String tuple1String = Utils.tupleToString(tuple1);

            return tupleString.compareTo(tuple1String);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
