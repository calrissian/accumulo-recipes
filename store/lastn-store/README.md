#What is the Last N Store?

Accumulo's Versioning iterator is great at keeping only some finite number of previous versions of a single column but it will not help with documents that have been partitioned over many columns so that cell-level security can be constrained.
