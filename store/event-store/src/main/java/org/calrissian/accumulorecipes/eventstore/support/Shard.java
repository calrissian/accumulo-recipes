package org.calrissian.accumulorecipes.eventstore.support;


import java.text.SimpleDateFormat;
import java.util.Date;

public class Shard {

    protected final Integer numPartitions;

    protected String delimiter = "_";

    /**
     * A proper date format should be lexicographically sortable
     */
    protected String dateFormat = "yyyyMMddhh";

    public Shard(Integer numPartitions) {
        this.numPartitions = numPartitions;
    }

    public String buildShard(long timestamp) {

        Date date = new Date(timestamp);
        return String.format("%s%s_%d", new SimpleDateFormat(dateFormat).format(date), delimiter, numPartitions);
    }

    public Integer getNumPartitions() {
        return numPartitions;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }
}
