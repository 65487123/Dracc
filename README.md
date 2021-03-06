# Dracc  (Distributed registry and configuration center)

基于Raft算法实现的分布式注册中心以及配置中心


## 特点

* 高可用,只支持集群部署
* 数据强一致性,集群节点之间数据同步基于Raft协议
* 高性能(相比其他基于强一致性算法实现的产品)  
  Raft算法基于长连接实现(基于Netty),并且做了很多优化,线程模型设计、代码实现细节注重性能
* 数据持久化保存
* 对其他组件无依赖(比如数据库什么的)
* 原生支持分布式锁。不用自己在客户端封装,自带相对安全的高性能可重入分布式锁。(由于网络的不确定性,实现绝对安全的分布式锁是不可能的，还是需要业务方做好相应的判断以及处理)  
但是, 我觉得绝大部分场景下是完全没必要用分布式锁的,业务模块实现上最好能不用分布式锁就不用分布式锁。


## 主要功能
* 服务注册
* 服务发现
* 服务监听
* 健康检查
* 发布配置
* 获取配置
* 分布式锁
* ......

## 部署步骤
1、拉取代码  
2、项目根目录执行mvn clean package  
3、进入 dracc-server/target 目录  
4、找到dracc-server-1.0.tar.gz 压缩包,拷贝到需要实际部署的目录中  
5、解压压缩包(windows直接右键选择解压到当前目录,Linux执行 tar -zvxf dracc-server-1.0.tar.gz)  
6、进入解压出的config目录,编辑cluster.properties文件,配置这个raft集群的所有节点ip(需要配置本地节点ip、远端节点ip,配置文件中有例子)  
7、进入解压出的bin目录,执行启动脚本(run.sh或run.bat是前台运行,start.sh是后台运行),后台运行产生的日志会输出到解压出的log目录中。  
成功启动半数以上raft节点,则这个raft集群为可用状态,他会自动选举出leader,并对外提供服务。



## 客户端使用教程

[Java客户端](https://github.com/65487123/Dracc/blob/main/dracc-javaclient/CLI_README.md)




