API
===

All APIs are HTTP RESTful. The are two type of APIs swarmstorage provided:
  - access object
  - maintenance

Access object
=============

Client can use swarmstorage by accessing OSS APIs of any nodes in the domain. 

### Write object

* URL 
  -	/swarm/oss/write
* Method
  - POST
* Params
  - blocktype
  - (optional)objecthash
  - (optional)baseobject
  - (optional)offset
* Body
  - Raw object content
* Return
  - JSON encode ObjectWriteResponse

### Read object

* URL 
  -	/swarm/oss/read
* Method
  - GET
* Params
  - objecthash
  - blocktype
* Body
  - Null
* Return
  - Raw object content

### Query object

* URL 
  -	/swarm/oss/query
* Method
  - GET
* Params
  - objecthash
  - blocktype
* Body
  - Null
* Return
  - JSON encode ObjectStatusResponse

Maintenance
===========

Maintenance APIs is used to execute maintenance tasks.

Monitor
-------

### Version

* URL 
  -	/swarm/monitor/version
* Method
  - GET
* Params
  - Null
* Body
  - Null
* Return
  - Plaintext swarmstorage version

### Query

* URL 
  -	/swarm/monitor/query
* Method
  - GET
* Params
  - name
* Body
  - Null
* Return
  - JSON encode MonitorQueryResponse

### All

* URL 
  -	/swarm/monitor/all
* Method
  - GET
* Params
  - Null
* Body
  - Null
* Return
  - JSON encode MonitorAllResponse

### System infomation

* URL 
  -	/swarm/monitor/sysinfo
* Method
  - GET
* Params
  - Null
* Body
  - Null
* Return
  - JSON encode MonitorSysinfoResponse

### Runtime variable

* URL 
  -	/swarm/monitor/variable
* Method
  - GET
* Params
  - name
* Body
  - Null
* Return
  - JSON encode MonitorVariableResponse

### Get alarms

* URL 
  -	/swarm/monitor/alarm
* Method
  - GET
* Params
  - Null
* Body
  - Null
* Return
  - JSON encode MonitorAlarmResponse

Admin
-----

This part will be rewritten after APIs are stable.

Currently, please see file modules/swarm/const.go and modules/swarm/admin_server.go for most information.


