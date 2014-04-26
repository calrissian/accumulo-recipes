package org.calrissian.accumulorecipes.geospatialstore.support;


import org.calrissian.accumulorecipes.geospatialstore.model.BoundingBox;

public class QuadTreeScanRange {

    private String minimum;
    private String maximum;

    private static String getNextId(BoundingBox quad) {
        return quad.getId().substring(0, quad.getId().length() - 1) + (char) (quad.getId().charAt(quad.getId().length() - 1) + 1);
    }

    public QuadTreeScanRange(BoundingBox quad) {
        this.minimum = quad.getId();
        this.maximum = getNextId(quad);
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

    public void setMinimum(String minimum) {
        this.minimum = minimum;
    }

    public String getMaximum() {
        return maximum;
    }

    public void setMaximum(String maximum) {
        this.maximum = maximum;
    }

    @Override
    public String toString() {
        return "QuadScanRange{" +
                "[" + minimum + "," + maximum + "]}";
    }
}
