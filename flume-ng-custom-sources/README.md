Flume Custom Sources
=============

Custom Sources
-------------------


### Directory Tailing Flume Source

DirTailPollableSource

DirTailEventDrivenSource


### Command

$ bin/flume-ng agent -n a1 -c conf -f conf/example.properties -Dflume.root.logger=DEBUG,console


Performance Results
-------------------

### DirTailPollableSource:

avg 250k/s CPU: avg 28% Mem: 70MB

### DirTailEventDrivenSource:

avg 150k/s CPU: avg 18% Mem: 10MB