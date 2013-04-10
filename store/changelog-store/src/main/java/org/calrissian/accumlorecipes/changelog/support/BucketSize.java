package org.calrissian.accumlorecipes.changelog.support;

public enum BucketSize {

    ONE_MIN(60000),
    FIVE_MINS(300000),
    TEN_MINS(600000),
    FIFTEEN_MINS(900000),
    HALF_HOUR(900000 * 2),
    ONE_HOUR(900000 * 4),
    DAY(900000 * 4 * 24);

    private long ms;
    private BucketSize(long ms) {
        this.ms = ms;
    }

    public long getMs() {
        return ms;
    }
}
