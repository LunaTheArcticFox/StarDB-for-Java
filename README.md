# StarDB for Java

A port of the [Python library](https://github.com/McSimp/StarDB) for manipulating Starbound database files to Java.
The code is loosely based on [yuedb](https://bitbucket.org/kyren/yuedb), which 
is written by one of the Starbound developers and is what the DB code in the 
game is based on as well.

Currently StarDB only supports read operations, but write operations may be 
added in the future.

## Usage

First, open the database file:
```java
AssetDatabase db = AssetDatabase.open("D:/Games/Steam/steamapps/common/Starbound/assets/packed.pak");
```

Alternatively, use a Path:
```java
AssetDatabase db = AssetDatabase.open(Paths.get("D:/Games/Steam/steamapps/common/Starbound/assets/packed.pak"));
```

You can then grab individual assets from the database as byte arrays:
```java
System.out.println(new String(db.getAsset("/player.config")));
```

You can also get a List of each file in the database as well as all broken files (see Current Issues):
```java
System.out.println(db.getBrokenFileList());
System.out.println(db.getFileList());
```

## Current Issues

There is a bug (or maybe a deliberate change) in the Starbound SHA256 
implementation, which results in the hash for every 55 character string to be 
incorrect. This means reading any file from an AssetsDatabase that has a 55 
character file path will not work.