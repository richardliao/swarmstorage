About CAP
=========

The [CAP theorem](http://en.wikipedia.org/wiki/CAP_theorem) states that any networked shared-data system can have at most two of three desirable properties:

 * consistency (C) equivalent to having a single up-to-date copy of the data;
 * high availability (A) of that data (for updates); and
 * tolerance to network partitions (P).

In larger distributed-scale systems, network partitions are a given; therefore, consistency and availability cannot be achieved at the same time.

All three properties are a matter of degree.


Effection of topology 
=====================

As a distributed storage system, the typical topology consist of:

 * storage nodes: connected by high speed interior network
 * access nodes: handle clients requests, between clients and interior nodes
 * client nodes: connect to access nodes by exterior network

If we despite client nodes, partitions is related to storage and access nodes, that are connected by interior network.

Access nodes act as a bridge between interior network and client nodes.

From client's perspective, consistency and availability is the apparence of the system, but partitions tolerance is seldom observed. What we really care about is client's perspective. If partition occurs between client node and access nodes, explicit exceptions raises in client application.

The most important partition occurs between access nodes and storage nodes, which directly impact consistency and availability of clients connecting to the access node. So, the key point is to limit the impact of partition between access nodes and storage nodes. 


Degrees of CAP
==============

The degree of partition can be approximately divided into bellowing levels(by severity):

 * isolated: access nodes can't contact all other interior nodes
 * group partitioned: some interior nodes can contact each other, while other interior nodes can contact each other, but not any one can contact between these group nodes
 * occasional node fail: some nodes containing the data access nodes need is not contactable
 * occasional call fail/timeout: some requests failed

The degree of consistent can be divided into bellowing levels(by severity):

 * data not exists
 * data is out of date
 * data is up to date

The degree of availability can be divided into bellowing types:

 * available for read
 * available for write
 * available for delete
 * available for other operations


Fight partition
===============

For swarmstorage, the different degree of partition has following impact on consistent and availability:

 * isolated: no consistent/available for w, consistent/available for r if any replica data is local node, else no
 * group partitioned: consistent/available for w, available for r, consistent for r if any replica data is in group, else no
 * occasional node fail: consistent/available for w, consistent/available for r
 * occasional call fail/timeout: consistent/available for w, consistent/available for r

The different degree of consistent of swarmstorage:

 * data not exists
 * data is up to date

The different degree of availability of swarmstorage:

 * available for read
 * available for write

Occasional and small partition seldom possible hurt consistency and availability of clients access. Even in large scale partition(group partitioned), swarmstorage can still keep CA for write, and partially keep A for read, and the inconsistent of read can be easily detected by application, since application only need to fix the inconsistent when access node response with data not exists.

The guaranteed CAP of swarmstorage covers much larger area than other distributed systems, the exceed guaranteed area is much frequently occur in distributed systems. With these  guarantees, swarmstorage can provide much higher reliability then others, while not harm any performance.

That is how swarmstorage fights CAP.

No delete
=========

Swarmstorage does not support delete operation. The reason is that delete operation is semantically conflict with write operation. 

To clarification, write operation in swarmstorage is adding new object or adding object based on existing object.

Block content is self consistent in swarmstorage, the only inconsistent is if data exists or not. Write operation increases data, while delete operation decreases data. To maximize consistent, hence minimize the effort to resolve conflict, we choose to keep data monotonically increasing.


High performance
================

Because the nature of self consistent of block content, swarmstorage spend little effort to resolve conflicts. Reading and writing can be pipelined through different nodes without additional operations. All data operation is guaranteed consistent without contact other nodes. Data flow is simplified as much as possible, which is the essential to guarantee of the high performance of data operations. 


Conclusion
==========

Swarmstorage is designed to maximize the reliability and the performance for massive storage in distributed system. This is implemented by providing only the essential features for client operations. This design choice achieves a very simplified and purified system, make we can focus on the most difficult problems of distributed system: CAP(consistency, availability and partition tolerance) and high performance.

