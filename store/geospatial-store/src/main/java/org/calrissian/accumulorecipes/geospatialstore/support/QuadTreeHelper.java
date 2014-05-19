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

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.*;

public class QuadTreeHelper {

    private static final BoundingBox WORLD = new BoundingBox(-180, -90, 360, 180);
    private static final int MAX_QUAD_SCAN_RANGES = 100;  //This is only a suggested maximum

    public String buildGeohash(Point2D.Double latLon, double maxPrecision) {
        return findQuad(latLon, WORLD, maxPrecision);
    }

    public Collection<QuadTreeScanRange> buildQueryRangesForBoundingBox(Rectangle2D.Double boundingBox, double maxPrecision) {
        return getInteriorQuads(boundingBox, MAX_QUAD_SCAN_RANGES, maxPrecision);
    }

    private boolean atMaxPrecision(BoundingBox quad, double maxPrecision) {
        return (quad.getCenterY() - quad.getMinY() < maxPrecision || quad.getCenterX() - quad.getMinX() < maxPrecision);

    }

    /**
     * Recursively look through each node in a quad tree to identify the quad with the given lat lon,
     * stopping when the resolution of the quadrant is less than the maxPrecision provided.
     */
    private String findQuad(Point2D.Double latLon, BoundingBox quad, double maxPrecision) {
        if (!quad.contains(latLon))
            return null;
        else if (atMaxPrecision(quad, maxPrecision))
            return quad.getId();

        String result = findQuad(latLon, quad.getNWQuad(), maxPrecision);
        if (result != null)
            return result;

        result = findQuad(latLon, quad.getNEQuad(), maxPrecision);
        if (result != null)
            return result;

        result = findQuad(latLon, quad.getSWQuad(), maxPrecision);
        if (result != null)
            return result;

        result = findQuad(latLon, quad.getSEQuad(), maxPrecision);
        if (result != null)
            return result;

        return null;
    }

    /**
     * Adds a quadrant a list of ranges.  Will merge the ranges to produce the smallest set of ranges possible.
     */
    private void addRange(BoundingBox quad, List<QuadTreeScanRange> ranges) {

        boolean placed = false;
        QuadTreeScanRange newRange = new QuadTreeScanRange(quad);

        for (int i = 0; i < ranges.size(); i++) {
            //Try to extend any of the current ranges given the current quad
            if (ranges.get(i).merge(newRange)) {
                placed = true;

                //if a range was extended, loop through the remaining elements to see if the new extension produced an
                //overlap by trying to merge the ranges.
                for (int j = i + 1; j < ranges.size(); j++) {
                    if (ranges.get(i).merge(ranges.get(j))) {
                        ranges.remove(j);
                        break;
                    }
                }
                break;
            }
        }


        if (!placed)
            ranges.add(newRange);

    }

    /**
     * Determines the best collection to place the supplied BoundingBox.  If all processing is done on the quad then it is
     * added to the list of completed {@link QuadTreeScanRange}s.  Otherwise it is added to the end of the toProcess queue for
     * further processing.
     */
    private void placeQuad(BoundingBox quad, Rectangle2D.Double boundingBox, List<QuadTreeScanRange> complete, Queue<BoundingBox> toProcess, double maxPrecision) {
        //if the quad is fully contained or the quad is at the maximum precision then simply add to the completed ranges.
        if (boundingBox.contains(quad) || atMaxPrecision(quad, maxPrecision)) {
            addRange(quad, complete);
        } else if (quad.contains(boundingBox) || quad.intersects(boundingBox)) {
            toProcess.offer(quad);
        }
    }

    private List<QuadTreeScanRange> getInteriorQuads(Rectangle2D.Double boundingBox, int maxQuads, double maxPrecision) {

        List<QuadTreeScanRange> complete = new ArrayList<QuadTreeScanRange>();
        ArrayDeque<BoundingBox> toProcess = new ArrayDeque<BoundingBox>(Arrays.asList(WORLD));

        //loop until the set of processing quads is depleted.
        while (toProcess.size() > 0) {

            BoundingBox processing = toProcess.poll();

            placeQuad(processing.getNEQuad(), boundingBox, complete, toProcess, maxPrecision);
            placeQuad(processing.getNWQuad(), boundingBox, complete, toProcess, maxPrecision);
            placeQuad(processing.getSEQuad(), boundingBox, complete, toProcess, maxPrecision);
            placeQuad(processing.getSWQuad(), boundingBox, complete, toProcess, maxPrecision);

            //while the size is too large remove the last element under the assumption that it is one of the smallest
            //quads containing a portion of the bounding box.  Then push this to the completed list to continue processing.
            while (complete.size() + toProcess.size() > maxQuads && toProcess.size() > 0) {
                addRange(toProcess.pollLast(), complete);
            }

        }

        return complete;
    }
}
