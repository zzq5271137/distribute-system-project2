We have fixed the bug of the previous version that it can't customise server secret.
For this version, you can customise the server secret.
(For example, to start the first server, the command line argument could be 
 "java -jar ActivityStreamerServerV2 -s whatever";
 to start another server, connecting to the network, the command line argument 
 could be "java -jar ActivityStreamerV2 -rp XXX -rh ABC -lp XXXX -s whatever").

The code for client and the use of ActivityStreamClient.jar are just the same as version 1.