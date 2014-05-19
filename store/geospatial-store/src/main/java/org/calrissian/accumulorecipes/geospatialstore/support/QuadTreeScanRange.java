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
package org.calrissian.accumulorecipes.geospatialstore.support;


import org.calrissian.accumulorecipes.geospatialstore.model.BoundingBox;

public class QuadTreeScanRange {

    private String minimum;
    private String maximum;

    public QuadTreeScanRange(BoundingBox quad) {
        this.minimum = quad.getId();
        this.maximum = getNextId(quad);
    }

    private static String getNextId(BoundingBox quad) {
        return quad.getId().substring(0, quad.getId().length() - 1) + (char) (quad.getId().charAt(quad.getId().length() - 1) + 1);
    }

    public boolean contains(BoundingBox quad) {
        return contains(quad.getId());
    }

    public boolean contains(String id) {
        return (minimum.compareTo(id) <= 0 && maximum.compareTo(id) >= 0);
    }

    public boolean overlaps(QuadTreeScanRange range) {
        return (this.contains(range.getMinimum()) || this.contains(range.getMaximum()) || range.contains(minimum) || range.contains(maximum));
    }

    public boolean merge(QuadTreeScanRange range) {
        if (overlaps(range)) {
            if (minimum.compareTo(range.getMinimum()) > 0) {
                minimum = range.getMinimum();
            }

            if (maximum.compareTo(range.getMaximum()) < 0) {
                maximum = range.getMaximum();
            }
            return true;
        }
        return false;


    }

    public String getMinimum() {
        return minimum;
    }

    public String getMaximum() {
        return maximum;
    }

    @Override
    public String toString() {
        return "QuadScanRange{" +
                "[" + minimum + "," + maximum + "]}";
    }
}
