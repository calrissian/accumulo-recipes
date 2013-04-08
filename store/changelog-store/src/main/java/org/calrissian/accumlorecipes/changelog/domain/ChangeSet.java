package org.calrissian.accumlorecipes.changelog.domain;

import org.calrissian.commons.domain.Tuple;

import java.util.Collection;

public class ChangeSet {

    Collection<Tuple> oldTuples;
    Collection<Tuple> newTuples;
}
