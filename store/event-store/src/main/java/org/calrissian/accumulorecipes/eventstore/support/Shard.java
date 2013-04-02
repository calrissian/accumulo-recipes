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

    public String buildShard(long timestamp, String uuid) {

        int partitionWidth = String.valueOf(numPartitions).length();
        Date date = new Date(timestamp);
        return String.format("%s%s%0" + partitionWidth + "d", new SimpleDateFormat(dateFormat).format(date),
                delimiter, (Math.abs(uuid.hashCode()) % numPartitions));
    }

    public String[] getRange(Date start, Date end) {

        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);

        return new String[] { sdf.format(start), sdf.format(end)};
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
