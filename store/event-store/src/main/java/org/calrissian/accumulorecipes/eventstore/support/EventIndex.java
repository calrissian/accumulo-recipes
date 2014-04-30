package org.calrissian.accumulorecipes.eventstore.support;

public class EventIndex {

  private String uuid;
  private Long timestamp;


  /**
   * Used when we know the timestamp and the uuid of the event we're referencing
   */
  public EventIndex(String uuid, long timestamp) {
    this.uuid = uuid;
    this.timestamp = timestamp;
  }

  /**
   * Used when we only know the uuid of the event we're referencing
   */
  public EventIndex(String uuid) {
    this.uuid = uuid;
  }

  public String getUuid() {
    return uuid;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    EventIndex that = (EventIndex) o;

    if (timestamp != that.timestamp) return false;
    if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = uuid != null ? uuid.hashCode() : 0;
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "EventIndex{" +
            "uuid='" + uuid + '\'' +
            ", timestamp=" + timestamp +
            '}';
  }
}
