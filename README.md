# Amazon's Mini Dynamo
=========================================================
***
* Simplified version of Dynamo (Key-Value storage), covers :
    * ID space partitioning/re-partitioning.
    * Ring based routing
    * Node joins
    * Quorum based replication
    * Recovery from replicated storage after failure

* SHA-1 hash function is used to lexically arrange nodes in a ring and find the location for a particular key to be stored.
* Each node maintains a successor and predecessor pointer for nodes in the ring.
* Content Provider is NoSQL storage that:
    - The first column should be named as “key”. This column is used to store all keys. 
    - The first column should be named as “value”. This column is used to store all values.


* Finger tables are not implemented.
* Failure Handling: All focus on correctness rather than performance

### Put buttons

- All Put* buttons operate the same way except that they insert different values with the same set of keys.
- When touched, it inserts 20 <key, value> pairs into your storage by using your content provider’s insert().
- Each touch of the button resets the sequence number to 0.
- The format of the <key, value> pairs is the following:
    - Key: the sequence number represented as a string starting from 0 (i.e., “0”, “1”, “2”, …, “19”.)
    - Value: string for each button concatenated by each sequence number.
        * For the button “Put1”, the values are “Put10”, “Put11”, “Put12”, etc.
        * For the button “Put2”, the values are “Put20”, “Put21”, “Put22”, etc.
        * For the button “Put3”, the values are “Put30”, “Put31”, “Put32”, etc.

### Get

- This button retrieves twenty keys, “0”, “1”, …, “19” and their corresponding values using content provider’s query() interface.


### LDump

- When touched, this button dumps and displays all the <key, value> pairs stored in your local partition of the node.

-------------------------
# Content Provider

- Content Provider deals with the main storage functionalities :

### Membership

- Each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.
- Assumption: There are always 3 nodes in the system and atmost 1 failure at a time.

### Quorum Replication

- The replication degree N is 3. This means that given a key, the key’s coordinator as well as the 2 successor nodes in the Dynamo ring should store the key.
- Both the reader quorum size R and the writer quorum size W is 2.
- The coordinator for a get/put request contacts other two nodes and get the votes. For each request, the coordinator returns to the requester whether the request was successful or not. If the coordinator fails to obtain the minimum number of votes, it returns an error.
- For write operations, all objects are versioned in order to distinguish stale copies from the most recent copy.
- For read operations, if the readers in the reader quorum have different versions of the same object, the coordinator will pick the most recent version and return it.

-------------------------------------------
## References

[1] Read about Dynamo [here](http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)

[2] Single best resource on Android, [Android dev](http://developer.android.com)

[3] Link to another note on Dynamo, [here](http://www.informationweek.com/software/information-management/amazon-dynamodb-big-datas-big-cloud-mome/232500104)
