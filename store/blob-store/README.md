#Accumulo Blob Store Recipe

## What is it?

The Accumulo Blob Store recipe allows bytes to be written as Accumulo mutations and effectively "streamed" to the Accumulo tablet servers so that larger streams never need to be placed entirely into memory. This makes it possible to insert and query and process very large streams (think PCAP files, large satellite geo imagery, videos). 

An extended version of the default blob store allows metadata (like created by, geotags, last updated time, size, etc...) to be stored with each blob, effectively turning it into a file store. Here's an example of how a file can be streamed from a local file system into Accumulo, where it's indexed and can be fetched easily.

## How do I use it?


###Example: Insert a file 
```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", "secret".getBytes());

// create a blob store with default chunk size of 1kb
AccumuloBlobStore blobStore = new AccumuloBlobStore(connector, 1024);  

// let's load a file and stream it into the blob store
File file = new File("/path/to/some/file.txt");
FileInputStream fis = new FileInputStream(file);

OutputStream storageStream = blobStore.store("/files/exampleFile.txt", "txt", file.lastModified(), "ABC");
IOUtils.copy(fis, storageStream);
storageStream.flush();
storageStream.close();
fis.close();
```

###Example: Getting a stored file
```java
Instance instance = new MockInstance();
Connector connector = instance.getConnector("root", "secret".getBytes());

// create a blob store with default chunk size of 1kb
AccumuloBlobStore blobStore = new AccumuloBlobStore(connector, 1024);  

// let's load a file and stream it into the blob store
File file = new File("/tmp/exampleFile.txt");

FileOutputStream fos = new FileOutputStream(file);
InputStream retrievalStream = blobStore.get("/files/exampleFile.txt", "txt", new Auths("ABC"));
IOUtils.copy(retrievalStream, fos);
fos.flush();
fos.close();
retrievalStream.close();
```
