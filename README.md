# doomdb
A LevelDB inspired database to learn concepts about DB storage and indexing engines (LSM, bloom filters, WAL...)

# Beginning the project

I have been reading a lot about databases, they are complex pieces and it's interesting to learn what good engineers have done to make the performant and safe.

I wanted to learn most of the main concepts too and I started with LevelDB and log structured merge trees.

We have few domain objects to deal with:

* SSTables stored on disk (**SSTable**) that we can consider partitions
* Indices files to speed up SSTables information retrieval (**SSTableIndex**)
* Write ahead logs on disk (**WAL**)
* Memory Index (**MemTableIndex**)
* Global in-memory index of data stored on disk plus the data that is being inserted into memory (**GlobalIndex**)

The process to store a new key-value in the DB is the following:

# Startup process

1. Search for WAL files that have data. Only one or none should be found so we'll start writing there or create a new one.  
2. Load all indexing files into memory. An index file must have keys, the file where the key is stored and the offset within the file.

Now if writes are received, they'll be written into the WAL file. If a read is received:

1. Read from the global index looking for the requested key
2. If not found in the global index, maybe it's in the WAL file. Read it.

1. Insert the data with any of the methods (CLI or HTTP)
2. Write the raw data in the ***WAL***
3. At the same time, check ***GlobalIndex***