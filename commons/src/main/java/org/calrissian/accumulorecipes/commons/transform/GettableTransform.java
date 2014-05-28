package org.calrissian.accumulorecipes.commons.transform;

import com.google.common.base.Function;
import org.calrissian.accumulorecipes.commons.domain.Gettable;

public class GettableTransform<T> implements Function<Gettable<T>, T> {
    @Override
    public T apply(Gettable<T> tGettable) {
        return tGettable.get();
    }
}
