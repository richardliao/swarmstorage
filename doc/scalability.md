Scalability
===========

Swarmstorage is designed to scale horizontally simply by adding nodes without notable performance degradation per node. The overall performance can be increased linearly as domain scales out.

Decentralized
=============

Swarmstorage is completely decentralized and purely peer to peer. 

Since there is no obvious centralized bottleneck, expanding requires no addition resources, the system is expected to expand indefinitely.

Note: in practice, of course it need some resources for expanding. For example, in star topology, the core switches capability is the required resource.


Shared nothing architecture
===========================

The node of swarmstorage is completely autonomous and independent each other. All nodes inside swarmstorage have equal role. A node acts completely by its own knowledge.

There is no master, no coordinator. There is even no consensus entire domain perspective. There is no centrally stored state information, so there is no single point of contention across the system, no single bottleneck to slow the system down.


Independent of domain size
==========================

Thanks to the benefit of consistent hash, virtual ticks of a device are invariable, the system can works perfectly even nodes only have partial perspective of entire domain. The only requirement is correlate nodes are consensus of the partial perspective.

For a specific node, the required knowledge about domain is limited to a fixed range, and information and data traffic are also limited to the range. That make the system may scale well because the demand on each node is independent of the total number of nodes.

