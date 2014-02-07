# StarDB for Java

A port of the [Python library](https://github.com/McSimp/StarDB) for manipulating Starbound database files to Java.
The code is loosely based on [yuedb](https://bitbucket.org/kyren/yuedb), which 
is written by one of the Starbound developers and is what the DB code in the 
game is based on as well.

Currently StarDB only supports read operations, but write operations may be 
added in the future.

## Usage

Coming soon!

## Current Issues

There is a bug (or maybe a deliberate change) in the Starbound SHA256 
implementation, which results in the hash for every 55 character string to be 
incorrect. This means reading any file from an AssetsDatabase that has a 55 
character file path will not work.

Also, this is not ready for use yet. It needs cleaning up before I'll consider
it a proper library. All the functionality is there, however.