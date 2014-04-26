#Accumulo GeoSpatial Store Recipe

It's often necessary to collapse multi-dimensional data down into a single dimension in Accumulo so that they can be scanned forward using some pre-determined range with an Accumulo scanner. The GeoSpatial store uses a space-filling  z-curve based on quad trees to generate a geo-hash at a predermined depth. It indexes events using this geohash so that the events themselves can be reconstructed when queried.

The purpose of the store is to find all of the possible events that are associated with 2-dimensional geo-points that lie within a given bounding box. 
