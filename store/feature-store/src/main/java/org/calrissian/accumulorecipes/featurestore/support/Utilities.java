package org.calrissian.accumulorecipes.featurestore.support;

import static org.apache.commons.lang.StringUtils.join;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;

public class Utilities {
    private Utilities() {
    }

    public static String combine(String... items) {
        if (items == null)
            return null;
        return join(items, NULL_BYTE);
    }
}
