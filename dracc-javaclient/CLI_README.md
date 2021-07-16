# JDracc

Dracc的java客户端


# 使用方法
1、maven依赖中添加JDracc的坐标(maven本地仓库需要有这个包,如果没有,需拉代码然后执行mvn install)

    <dependency>
        <artifactId>dracc-common</artifactId>
        <groupId>com.lzp.dracc</groupId>
        <version>1.0</version>
    </dependency>

2、创建JDracc对象

    //dracc-server的所有节点IP
    String ipAndPortA = "127.0.0.1:6667";
    String ipAndPortB = "127.0.0.1:6668";
    String ipAndPortC = "127.0.0.1:6669";
    //不带超时时间的客户端
    DraccClient draccClient = new JDracc(ipAndPortA, ipAndPortB, ipAndPortC);
    //带超时时间的客户端,超时时间为3s(请求server时,超过3s没响应就会抛异常)
    DraccClient draccClient1 = new JDracc(3000, ipAndPortA, ipAndPortB, ipAndPortC);

3、用这个DraccClient实例操作Dracc-server。具体的使用方法参考[DraccClient](https://github.com/65487123/Dracc/blob/main/dracc-javaclient/src/main/java/com/lzp/dracc/javaclient/api/DraccClient.java)这个接口的java doc。  

在[dracc-javaclient/src/test](https://github.com/65487123/Dracc/blob/main/dracc-javaclient/src/test/java/ClientTest.java)目录下有我写的例子,包括功能测试以及性能测试,代码拉下来也能直接跑
        





