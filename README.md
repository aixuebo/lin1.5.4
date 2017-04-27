Apache Kylin
============

[![Build Status](https://travis-ci.org/apache/kylin.svg?branch=master)](https://travis-ci.org/apache/kylin)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

> Extreme OLAP Engine for Big Data

Apache Kylin is an open source Distributed Analytics Engine, contributed by eBay Inc., provides SQL interface and multi-dimensional analysis (OLAP) on Hadoop supporting extremely large datasets.

For more details, see the website [http://kylin.apache.org](http://kylin.apache.org).

Documentation
=============
Please refer to [http://kylin.apache.org/docs15/](http://kylin.apache.org/docs15/).

Get Help
============
The fastest way to get response from our developers is to send email to our mail list <dev@kylin.apache.org>,   
and remember to subscribe our mail list via <dev-subscribe@kylin.apache.org>

License
============
Please refer to [LICENSE](https://github.com/apache/kylin/blob/master/LICENSE) file.

kylin1.5.4.1

org.apache.kylin.query.enumerator.OLAPEnumerator 调用search方法
    ITupleIterator iterator = storageEngine.search(olapContext.storageContext, sqlDigest, olapContext.returnTupleInfo);//查询sql--返回迭代器
org.apache.kylin.storage.gtrecord.GTCubeStorageQueryBase如何查询sql
org.apache.kylin.storage.hbase.cube.v2.CubeHBaseScanRPC执行hbase的查询