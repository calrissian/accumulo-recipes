#Temporal Last N Store Recipe

This recipe is similar in theory to that of the regular last n store. It's purpose is to provide some window into events that it's storing where the "last n" events will be returned in order of recency. This implementation of the last n is unique in that it will keep all data but allows the last n to be queried with a time range. 

Another unique property of this store is that it allows the last n to be queried (still in chronological order) across several different groups). 

##Why is this useful?

Let's say you have a system where users have several different categories of "news feed" updates that they can subscribe to on a dashboard. Each of these categories can hold updates like "blog post activity" on their favorite blogs, "world news" updates, and "email updates". The temporal last n store allows many different ways to provide the feeds to each user. First, each feed can be queried separately to find the last n and they can be queried over periods of time, currently or historically to see what updates were occurring during different times. Further, it provides the ability to join the updates across different feeds. For instance, this is useful when we want a consolidated feed that will show the last 100 updates for all three categories. 

Now let's say we have a system where we are monitoring possible malicious activity and we have several different categories of feeds that we are collecting. Perhaps some categories can be systems themselves providing the feeds. Some categories may be specific data types (router logs vs http logs). For this case, a user would want to correllate the events in history over some window (the n value) to place the events in chronological order so that they can draw conclusions.



