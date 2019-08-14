Version 1:
ActivityStreamerClient.jar and ActivityStreamerServer.jar fles work as normal.

Tips: The JFrame is just capable of sending and displaying received
       activity message. To send an activity message, a valid JSON
       structure is needed (like {"say":"hello world","name":"user No.1"}).
       
       For starting a client:
       (1). -u XXX means you want to first register as user XXX and then login
            (secret is automatically allocated and displaied on screen)
       (2). -u XXX -s ZZZ means you want to directly login as user XXX with secret ZZZ
       (3). without user-parameters (just java -jar ActivityStreamerClient.jar
            -rh ABC -rp 123) means you want to login as anonymous
            
       For starting multiple servers:
       The secret is group666

Version 2:
We have fixed the bug of the previous version that it can't customise server secret.
For this version, you can customise the server secret.
(For example, to start the first server, the command line argument could be 
 "java -jar ActivityStreamerServerV2 -s whatever";
 to start another server, connecting to the network, the command line argument 
 could be "java -jar ActivityStreamerV2 -rp XXX -rh ABC -lp XXXX -s whatever").

The code for client and the use of ActivityStreamClient.jar are just the same as version 1.