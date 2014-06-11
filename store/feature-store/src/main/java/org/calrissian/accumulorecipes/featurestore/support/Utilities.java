package org.calrissian.accumulorecipes.featurestore.support;

import static org.apache.commons.lang.StringUtils.join;
import static org.calrissian.accumulorecipes.commons.support.Constants.DELIM;

public class Utilities {
    private Utilities() {
    }

    public static String combine(String... items) {
        if (items == null)
            return null;
        return join(items, DELIM);
    }
}
