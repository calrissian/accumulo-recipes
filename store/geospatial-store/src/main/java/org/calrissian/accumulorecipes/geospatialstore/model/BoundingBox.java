package org.calrissian.accumulorecipes.geospatialstore.model;

import java.awt.geom.Rectangle2D;

public class BoundingBox extends Rectangle2D.Double {

    String id;

    public BoundingBox(double x, double y, double w, double h) {
        this(x, y, w, h, "");
    }

    public BoundingBox(double x, double y, double w, double h, String id) {
        super(x, y, w, h);
        this.id = id;
    }

    public BoundingBox getNWQuad() {
        double topX = getMinX();
        double topY = getCenterY();
        double width = getCenterX() - topX;
        double height = getMaxY() - topY;

        return new BoundingBox(topX, topY, width, height, id + 1);
    }

    public BoundingBox getNEQuad() {

        double topX = getCenterX();
        double topY = getCenterY();
        double width = getMaxX() - topX;
        double height = getMaxY() - topY;

        return new BoundingBox(topX, topY, width, height, id + 2);
    }

    public BoundingBox getSWQuad() {

        double topX = getMinX();
        double topY = getMinY();
        double width = getCenterX() - topX;
        double height = getCenterY() - topY;

        return new BoundingBox(topX, topY, width, height, id + 4);
    }

    public BoundingBox getSEQuad() {

        double topX = getCenterX();
        double topY = getMinY();
        double width = getMaxX() - topX;
        double height = getCenterY() - topY;

        return new BoundingBox(topX, topY, width, height, id + 3);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "BoundingBox{" +
                "id='" + id + '\'' +
                '}';
    }
}
