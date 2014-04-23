#What is the Last N Store?

Accumulo's Versioning iterator is great at keeping only some finite number of previous versions of a single column but it will not help with documents that have been partitioned over many columns so that cell-level security can be constrained. This store effectively demonstrates how a count-based eviction can still be implemented using filters that store state so that the underlying keys/values of documents can be filtered appropriately. 

#Okay, but what's the purpose?

There are many use-cases when this could be an effective solution. Specifically, when passing events through a CEP engine and alerting on different correlations of those events, it's possible that you may want a cache to store the last n events that may have fired on an alerting stream so that they can see if there have been recent events, as well as which properties make up those events. Another use-case could be for a "news feed" where you may want to keep events that occurred in your system but you want to limit them by count instead of time- perhaps grouping them by user, or by event type.



