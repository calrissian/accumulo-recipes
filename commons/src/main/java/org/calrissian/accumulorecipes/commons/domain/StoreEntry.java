/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.commons.domain;


import org.calrissian.mango.domain.AbstractTupleCollection;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.TupleCollection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import static java.lang.System.currentTimeMillis;

/**
 * A store entry acts as a useful common business object for representing different types of models. An optional time
 * dimension can be set directly or left untouched (defaulting in current time).
 */
public class StoreEntry extends AbstractTupleCollection {

  protected final String id;
  protected final long timestamp; // in Millis

  /**
   * New store entry with random UUID and timestamp defaulted to current time
   */
  public StoreEntry() {
    this(UUID.randomUUID().toString());
  }

  /**
   * New store entry with ID. Timestamp defaults to current time.
   *
   * @param id
   */
  public StoreEntry(String id) {
    this(id, currentTimeMillis());
  }

  /**
   * New store entry with ID and a timestamp
   *
   * @param id
   * @param timestamp
   */
  public StoreEntry(String id, long timestamp) {
    this.id = id;
    this.timestamp = timestamp;
  }

  /**
   * Accessor for Id
   *
   * @return
   */
  public String getId() {
    return id;
  }

  /**
   * Accessor for timestamp
   *
   * @return
   */
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    StoreEntry that = (StoreEntry) o;

    if (timestamp != that.timestamp) return false;
    if (id != null ? !id.equals(that.id) : that.id != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (id != null ? id.hashCode() : 0);
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "StoreEntry{" +
            "id='" + id + '\'' +
            ", timestamp=" + timestamp +
            '}';
  }
}
