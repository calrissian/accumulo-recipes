
#Accumulo Blob Store Recipe

## What is it?

The Accumulo Blob Store recipe allows streams to be written as Accumulo mutations and effectively "streamed" to the Accumulo tablet servers so that streams never need to be placed into memory. This makes it possible to insert and query and process very large streams (PCAP files, large satellite geo imagery). 

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
File file = new File("/path/to/output/file.txt");

FileOutputStream fos = new FileOutputStream("/tmp/exampleFile.txt");
InputStream retrievalStream = blobStore.get("/files/exampleFile.txt", "txt", new Auths("ABC"));
IOUtils.copy(retrievalStream, fos);
fos.flush();
fos.close();
retrievalStream.close();
```
