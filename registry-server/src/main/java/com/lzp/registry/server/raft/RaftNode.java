
/* Copyright zeping lu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.lzp.registry.server.raft;

import com.lzp.registry.common.constant.Cons;
import com.lzp.registry.common.util.PropertyUtil;
import com.lzp.registry.common.util.ThreadFactoryImpl;
import com.lzp.registry.server.netty.NettyClient;
import com.lzp.registry.server.netty.NettyServer;
import com.lzp.registry.server.util.CountDownLatch;
import com.sun.jndi.ldap.Connection;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.relation.Role;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Description:raft节点
 *
 * @author: Zeping Lu
 * @date: 2021/3/16 16:45
 */
public class RaftNode {

    /**
     * 延迟任务
     */
    private static class DelayTask implements Delayed,Runnable{

        Runnable runnable;
        long deadline;
        long delay;

        DelayTask(Runnable runnable, long delay) {
            this.runnable = runnable;
            this.deadline = System.currentTimeMillis() + (this.delay = delay);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public void run() {
            runnable.run();
        }
    }

    /**
     * 重写equals方法以便放入set中
     */
    private static class Connection {
        private Channel channel;

        Connection(Channel channel) {
            this.channel = channel;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Connection) {
                Connection connection = (Connection) o;
                InetSocketAddress inetSocketAddress = (InetSocketAddress) this.channel.remoteAddress();
                InetSocketAddress inetSocketAddress1 = (InetSocketAddress) connection.channel.remoteAddress();
                return getIp(inetSocketAddress).equals(getIp(inetSocketAddress1))
                        && getPort(inetSocketAddress) == getPort(inetSocketAddress1);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return 1;
        }

        private String getIp(InetSocketAddress inetSocketAddress) {
            return inetSocketAddress.getAddress().getHostAddress();
        }

        private int getPort(InetSocketAddress inetSocketAddress) {
            return inetSocketAddress.getPort();
        }
    }

    private final static Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);

    /**
     * 真正的数据(状态机)
     */
    private static Map<String, Set<String>> data = new HashMap<>();

    /**
     * 任期
     */
    private static long term;

    /**
     * 当前节点角色
     */
    private static volatile String role;


    /**
     * 执行超时选举任务的线程池
     */
    private static ExecutorService timeoutToElectionExecutor;

    /**
     * 执行心跳任务的线程池
     */
    private static ExecutorService heartBeatExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryImpl("heatbeat"));

    /**
     * 超时选举任务
     */
    private static DelayTask electionTask;

    /**
     * 主节点才有值
     */
    private static Set<Connection> slaveChannels = new CopyOnWriteArraySet();

    /**
     * 半数
     */
    private static short halfCount;

    /**
     * key是commandid,value是这个command向从节点发送以后收到的结果(是否半数成功)
     */
    private static Map<String, CountDownLatch> cidAndResultMap = new HashMap<>();

    /**
     * id计数器
     */
    private static AtomicInteger commandIdCunter = new AtomicInteger();

    static {
        Properties clusterProperties = PropertyUtil.getProperties(Cons.CLU_PRO);
        String[] remoteNodeIps = clusterProperties.getProperty("remoteRaftNodes").split(",");
        halfCount = (short) (remoteNodeIps.length % 2 == 0 ? remoteNodeIps.length / 2 : remoteNodeIps.length / 2 + 1);
        role = Cons.FOLLOWER;
        String[] localIpAndPort = clusterProperties.getProperty("localRaftNode").split(Cons.COLON);
        NettyServer.start(localIpAndPort[0], Integer.parseInt(localIpAndPort[1]));
        timeoutToElectionExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new DelayQueue(),
                new ThreadFactoryImpl("timeout to election"));
        timeoutToElectionExecutor.execute(() -> {});
        electionTask = new DelayTask(() -> startElection(remoteNodeIps), ThreadLocalRandom.current().nextInt(12000, 18000));
        timeoutToElectionExecutor.execute(electionTask);
    }

    /**
     * 发起选举
     */
    private static void startElection(String[] remoteNodeIps) {
        role = Cons.CANDIDATE;
        String voteRequestId = getCommandId();
        CountDownLatch countDownLatch = new CountDownLatch(halfCount);
        cidAndResultMap.put(voteRequestId, countDownLatch);
        ExecutorService threadPoolExecutor = new ThreadPoolExecutor(remoteNodeIps.length, remoteNodeIps.length, 0,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryImpl("ask for vote"));
        for (String remoteNodeIp : remoteNodeIps) {
            String[] ipAndPort = remoteNodeIp.split(Cons.COLON);
            threadPoolExecutor.execute(() -> {
                Channel channel = NettyClient.getChannelAndRequestForVote(ipAndPort[0], Integer
                        .parseInt(ipAndPort[1]), term = LogService.increaseCurrentTerm(term), LogService.getLogIndex());
                if (channel != null) {
                    if (slaveChannels.add(new Connection(channel))) {
                        addListenerOnChannel(channel);
                    } else {
                        channel.close();
                    }
                }
            });
        }
        try {
            if (countDownLatch.await(ThreadLocalRandom.current().nextLong(3000, 5000), TimeUnit.MILLISECONDS)) {
                upgradToLeader();
            }
        } catch (InterruptedException e) {
            LOGGER.info("Interrupted while waiting for vote", e);
        }

    }

    /**
     * 升级成主节点
     */
    private static void upgradToLeader() {
        role = Cons.LEADER;
        heartBeatExecutor.execute(() -> {
            while (true) {
                byte[] emptyPackage = new byte[0];
                for (Connection connection : slaveChannels) {
                    connection.channel.writeAndFlush(emptyPackage);
                }
            }
        });
    }

    /**
     * 监听channel,当channel关闭后,根据具体情况选择是否重连
     */
    private static void addListenerOnChannel(Channel channel) {
        channel.closeFuture().addListener(future -> {
            slaveChannels.remove(new Connection(channel));
            if (Cons.LEADER.equals(RaftNode.getRole())) {
                InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.remoteAddress();
                Channel newChannel = NettyClient.getChannelAndCheckSync(inetSocketAddress.getAddress()
                        .getHostAddress(), inetSocketAddress.getPort(), term, LogService.getLogIndex());
                if (newChannel != null) {
                    if (slaveChannels.add(new Connection(newChannel))){
                        addListenerOnChannel(newChannel);
                    }else {
                        newChannel.close();
                    }
                }
            }
        });
    }

    /**
     * 获取当前角色
     */
    public static String getRole() {
        return role;
    }

    /**
     * 设置当前角色
     */
    public static void setRole(String newRole) {
        role = newRole;
    }

    /**
     * 重置计时器
     */
    public static void ResetTimer() {
        electionTask.deadline = System.currentTimeMillis() + electionTask.delay;
    }

    /**
     * 获取命令唯一id
     */
    public static String getCommandId() {
        return Integer.toString(commandIdCunter.getAndIncrement());
    }

    /**
     * 获取命令id以及相应结果map
     */
    public static Map<String, CountDownLatch> getCidAndResultMap() {
        return cidAndResultMap;
    }

    /**
     * 获取当前任期
     */
    public static long getTerm(){
        return term;
    }
}
