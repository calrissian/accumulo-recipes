#Temporal Last N Store Recipe

This recipe is similar in theory to that of the regular last n store. It's purpose is to provide some window into events that it's storing where the "last n" events will be returned in order of recency. This implementation of the last n is unique in that it will keep all data but allows the last n to be queried with a time range. While the regular last n store automatically evicts items by count, both stores can evict by time if an age-off filter is configured on the table.

Another unique property of this store is that it allows the last n to be queried (still in chronological order) across several different groups). 

##Why is this useful?

Let's say you have a system where users have several different categories of "news feed" updates that they can subscribe to on a dashboard. Each of these categories can hold updates like "blog post activity" on their favorite blogs, "world news" updates, and "email updates". The temporal last n store allows many different ways to provide the feeds to each user. First, each feed can be queried separately to find the last n and they can be queried over periods of time, currently or historically to see what updates were occurring during different times. Further, it provides the ability to join the updates across different feeds. For instance, this is useful when we want a consolidated feed that will show the last 100 updates for all three categories. 

Now let's say we have a system where we are monitoring possible malicious activity and we have several different categories of feeds that we are collecting. Perhaps some categories can be systems themselves providing the feeds. Some categories may be specific data formats  (router logs vs http logs). Perhaps a user would want to correllate the events in history over some window (the n value) to place the events in chronological order so that they can draw conclusions.


##Adding data to the store

First we'll construct a few StoreEntry objects that we can place in the store.

```java
Event blogUpdate = new BaseEvent(UUID.randomUUID().toString());
blogUpdate.put(new Attribute("link", "http://blogs-r-cool.com/", ""));
blogUpdate.put(new Attribute("owner", "John Doe", ""));
blogUpdate.put(new Attribute("updateType", "New Content Added", ""));
blogUpdate.put(new Attribute("contentName", "The people we know", ""));

Event worldNews = new BaseEvent(UUID.randomUUID().toStirng());
worldNews.add(new Attribute("provider", "CNN", ""));
worldNews.add(new Attribute("headline", "Burglary in the grocery store", ""));
worldNews.add(new Attribute("reporter", "Jane Doe", ""));

Event emailUpdate = new BaseEvent(UUID.randomUUID().toString());
emailUpdate.add(new Attribute("from", "thisguy@gmail.com", ""));
emailUpdate.add(new Attribute("subject", "Things you should see before age 50", ""));
emailUpdate.add(new Attribute("to", "yournamehere@gmail.com", ""));
```

Now let's create a store and add the events above to the store under the appropriate groups.

```java
Instance instance = new MockInstance();
Connector connector  = instance.getConnector("root", "".getBytes());
AccumuloTemporalLastNStore store = new AccumuloTemporalLastNStore(connector);

store.put("userName|blogUpdates", blogUpdate);
store.put("userName|worldNews", worldNews);
store.put("userName|emailUpdates", emailUpdates);
```

So now we have our events in the stored grouped logically by username and feed category. 

##Querying the events

The events contained in the store will always be returned in chronological order. However, it's up to you how you want to ask for them. Events get queried for some list of groups.

```java
String[] groups = new new String[] { "userName|blogUpdates", "userName|worldNews", "userName|emailUpdates" };

// set our time range to the last hour
Date start = new Date(System.currentTimeMillis() - (60 * 60 * 1000));
Date stop = new Date();
Iterable<Event> lastNEntries = store.get(start, stop, Arrays.asList(groups), 100, new Auths());
```

In the example above, we are querying the last 100 events across the groups 'userName|blogUpdates', 'userName|worldNews', and 'userName|emailUpdates'. This will merge the last n feeds together and provide a holistic view.
