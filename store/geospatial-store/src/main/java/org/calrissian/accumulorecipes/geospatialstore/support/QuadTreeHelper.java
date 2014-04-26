package org.calrissian.accumulorecipes.geospatialstore.support;

import org.calrissian.accumulorecipes.geospatialstore.model.BoundingBox;

import java.awt.*;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.*;
import java.util.List;
import java.util.Queue;

public class QuadTreeHelper {

    private static final BoundingBox WORLD = new BoundingBox(-180, -90, 360, 180);
    private static final int MAX_QUAD_SCAN_RANGES = 100;  //This is only a suggested maximum

    public String buildGeohash(Point2D.Double latLon, double maxPrecision) {
        return findQuad(latLon, WORLD, maxPrecision);
    }

    public Collection<QuadTreeScanRange> buildQueryRangesForBoundingBox(Rectangle2D.Double boundingBox, double maxPrecision) {
        return getInteriorQuads(boundingBox, MAX_QUAD_SCAN_RANGES, maxPrecision);
    }

    public Collection<QuadTreeScanRange> buildQueryRangesForBounds(Point2D.Double lowerLeft, Point2D.Double upperRight, double maxPrecision) {

        return buildQueryRangesForBoundingBox(
                new Rectangle.Double(
                        lowerLeft.getX(),
                        lowerLeft.getY(),
                        upperRight.getX() - lowerLeft.getX(),
                        upperRight.getY() - lowerLeft.getY()
                ), maxPrecision
        );
    }


    private boolean atMaxPrecision(BoundingBox quad, double maxPrecision) {

        return (quad.getCenterY() - quad.getMinY() < maxPrecision || quad.getCenterX() - quad.getMinX() < maxPrecision);

    }

    /**
     * Recursively look through each node in a quad tree to identify the quad with the given lat lon,
     * stopping when the resolution of the quadrant is less than the maxPrecision provided.
     *
     * @param latLon
     * @param quad
     * @param maxPrecision
     * @return
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
     *
     * @param quad
     * @param ranges
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
     *
     * @param quad
     * @param boundingBox
     * @param complete
     * @param toProcess
     * @param maxPrecision
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

    /**
     * Recursive method to identify all quadrants that a particular bounding box contains or intersects.
     *
     * @param quad
     * @param boundingBox
     * @param maxPrecision
     * @return
     */
    @Deprecated
    private List<String> getInteriorQuads(BoundingBox quad, Rectangle2D.Double boundingBox, double maxPrecision) {


        //If the quad is fully contained then simply return the entire quad.
        if (boundingBox.contains(quad))
            return Arrays.asList(quad.getId());

        //If the quad intersects or fully contains the box then recurse to get a list of smaller quads.
        if (quad.contains(boundingBox) || quad.intersects(boundingBox)) {

            //if max depth has been reached then simply return the quad in question.
            if (atMaxPrecision(quad, maxPrecision))
                return Arrays.asList(quad.getId());

            List<String> results = new ArrayList<String>();

            results.addAll(getInteriorQuads(quad.getNEQuad(), boundingBox, maxPrecision));
            results.addAll(getInteriorQuads(quad.getNWQuad(), boundingBox, maxPrecision));
            results.addAll(getInteriorQuads(quad.getSEQuad(), boundingBox, maxPrecision));
            results.addAll(getInteriorQuads(quad.getSWQuad(), boundingBox, maxPrecision));

            return results;
        }

        return Collections.emptyList();
    }


}
