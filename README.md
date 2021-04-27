# Registry  分布式注册中心


基于raft算法实现的分布式注册中心


## 演示
![walden](https://raw.github.com/meolu/Walden/master/static/screenshots/walden.gif)


## 特点

* 高可用,只支持集群部署
* 数据强一致性,集群节点之间数据同步基于raft协议
* 高性能(相比其他基于强一致性算法实现的服务)
  raft算法基于长连接实现,日志复制、数据同步、心跳等过程
  做了很多优化,线程模型设计、代码实现注重性能
* 数据持久化保存
* 对其他组件无依赖(比如数据库什么的)


## 一、安装


## 二、快速开始






最后，当然希望你可以给此项目提个pull request，目前只有一个bootstrap的默认模板：(


## to do list

* 文档搜索
* 文档删除，重命名UI化


## CHANGELOG
瓦尔登的版本记录：[CHANGELOG](https://github.com/meolu/walden/blob/master/CHANGELOG.md)



