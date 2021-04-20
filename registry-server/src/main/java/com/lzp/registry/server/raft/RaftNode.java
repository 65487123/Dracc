
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
import com.lzp.registry.server.netty.CoreHandler;
import com.lzp.registry.server.util.CountDownLatch;
import com.lzp.registry.server.util.DataSearialUtil;
import com.lzp.registry.server.util.LogoUtil;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final static Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);

    /**
     * 当前节点角色
     */
    private static volatile String role = Cons.FOLLOWER;

    /**
     * 执行超时选举任务的线程池
     */
    private static final ExecutorService TIMEOUT_TO_ELECTION_EXECUTOR;

    /**
     * 执行心跳任务的线程池
     */
    private static ExecutorService heartBeatExecutor;

    /**
     * 超时选举任务
     */
    private static DelayTask electionTask;

    /**
     * id计数器
     */
    private static final AtomicInteger COMMAND_ID_COUNTER = new AtomicInteger();

    /**
     * 任期
     */
    public static long term;

    /**
     * 主节点才有值
     */
    public static Map<String, List<Channel>> termAndSlaveChannels;

    /**
     * 半数
     */
    public static final short HALF_COUNT;

    /**
     * key是commandid,value是这个command向从节点发送以后收到的结果(是否半数成功)
     */
    public static Map<String, CountDownLatch> cidAndResultMap = new ConcurrentHashMap<>();

    /**
     * 真正的数据(状态机)
     */
    public static Map<String, Set<String>> data = new HashMap<>();

    static {
        Properties clusterProperties = PropertyUtil.getProperties(Cons.CLU_PRO);
        String localNode = clusterProperties.getProperty("localRaftNode");
        LOGGER.info("server:'{}' is starting", localNode);
        term = Long.parseLong(LogService.getTerm());
        String[] remoteNodeIps = clusterProperties.getProperty("peerRaftNodes").split(Cons.COMMA);
        HALF_COUNT = (short) (remoteNodeIps.length % 2 == 0 ? remoteNodeIps.length / 2 : remoteNodeIps.length / 2 + 1);
        String[] localIpAndPort = localNode.split(Cons.COLON);
        NettyServer.start(localIpAndPort[0], Integer.parseInt(localIpAndPort[1]));
        TIMEOUT_TO_ELECTION_EXECUTOR = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new DelayQueue(),
                new ThreadFactoryImpl("timeout to election"));
        TIMEOUT_TO_ELECTION_EXECUTOR.execute(() -> {
        });
        electionTask = new DelayTask(() -> {
            LOGGER.info("heartbeat timed out, initiate an election");
            startElection(remoteNodeIps);
        }, ThreadLocalRandom.current().nextInt(12000, 18000));
        TIMEOUT_TO_ELECTION_EXECUTOR.execute(electionTask);
    }



    /**
     * 发起选举
     */
    private static void startElection(String[] remoteNodeIps) {
        updateTermAndSlaveChannels();
        role = Cons.CANDIDATE;
        String voteRequestId = getCommandId();
        CountDownLatch countDownLatch = new CountDownLatch(HALF_COUNT);
        cidAndResultMap.put(voteRequestId, countDownLatch);
        ExecutorService threadPoolExecutor = new ThreadPoolExecutor(remoteNodeIps.length, remoteNodeIps.length, 0,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryImpl("ask for vote"));
        for (String remoteNodeIp : remoteNodeIps) {
            String[] ipAndPort = remoteNodeIp.split(Cons.COLON);
            threadPoolExecutor.execute(() -> sendRpcAndSaveChannel(term, voteRequestId, ipAndPort[0], ipAndPort[1]));
        }
        try {
            if (countDownLatch.await(ThreadLocalRandom.current().nextLong(3500, 5000), TimeUnit.MILLISECONDS)) {
                upgradToLeader(Long.toString(term));
            } else {
                LOGGER.info("The election timed out, re-launch");
                startElection(remoteNodeIps);
            }
        } catch (InterruptedException e) {
            LOGGER.info("Interrupted while waiting for vote", e);
        }
    }


    /**
     * 发起投票并保存连接
     */
    private static void sendRpcAndSaveChannel(long currentTerm, String voteRequestId, String ip, String port) {
        Channel channel = NettyClient.getChannelAndRequestForVote(voteRequestId, ip, Integer
                .parseInt(port), currentTerm, LogService.getCommittedLogIndex(), LogService.getUncommittedLogSize());
        if (channel != null) {
            List<Channel> channelsToSlave;
            if ((channelsToSlave = termAndSlaveChannels.get(Long.toString(currentTerm))) != null) {
                channelsToSlave.add(channel);
                addListenerOnChannel(channel, term);
            }
        }
    }


    /**
     * 断开旧连接并从容器中移除,增加装新连接的容器,发起选举时会执行此方法
     */
    private static void updateTermAndSlaveChannels() {
        if (termAndSlaveChannels != null) {
            List<Channel> oldChannels = termAndSlaveChannels.remove(Long.toString(term));
            for (Channel channel : oldChannels) {
                channel.close();
            }
        } else {
            termAndSlaveChannels = new ConcurrentHashMap<>();
        }
        term = LogService.increaseCurrentTerm(term);
        termAndSlaveChannels.put(Long.toString(term), new CopyOnWriteArrayList<>());
    }


    /**
     * 升级成主节点
     */
    private static void upgradToLeader(String term) {
        LOGGER.info("successful election, upgrade to the master node");
        role = Cons.LEADER;
        CoreHandler.slaves = termAndSlaveChannels.get(term);
        heartBeatExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactoryImpl("heatbeat"));
        heartBeatExecutor.execute(() -> {
            while (true) {
                byte[] emptyPackage = new byte[0];
                for (Channel channel : CoreHandler.slaves) {
                    channel.writeAndFlush(emptyPackage);
                }
                try {
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
    }


    /**
     * 当主节点收到更高任期的消息时(网络分区恢复后)或者候选者发现已经有leader了,
     * 或者遇到特殊情况,为了防止出现数据不一致,
     * 会执行此方法,降级为从节点
     *
     * @param newTerm 新任期
     */
    public static void downgradeToSlaveNode(long newTerm) {
        LOGGER.info("downgrade to slave node");
        long preTerm = RaftNode.updateTerm(newTerm);
        List<Channel> oldChannels = termAndSlaveChannels.remove(Long.toString(preTerm));
        for (Channel channel : oldChannels) {
            channel.close();
        }
        role = Cons.FOLLOWER;
        heartBeatExecutor.shutdownNow();
        CoreHandler.resetReplicationThreadPool();
        LogService.clearUncommittedEntry();
        electionTask = new DelayTask(() -> {
            LOGGER.info("heartbeat timed out, initiate an election");
            startElection(PropertyUtil.getProperties(Cons.CLU_PRO)
                    .getProperty("localRaftNode").split(Cons.COLON));
        }, ThreadLocalRandom.current().nextInt(12000, 18000));
        TIMEOUT_TO_ELECTION_EXECUTOR.execute(electionTask);
    }



    /**
     * 监听channel,当channel关闭后,根据具体情况选择是否重连
     */
    private static void addListenerOnChannel(Channel channel, long term) {
        channel.closeFuture().addListener(future -> {
            if (Cons.LEADER.equals(RaftNode.getRole()) && RaftNode.term == term) {
                InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.remoteAddress();
                Channel newChannel = NettyClient.getChannelAndAskForSync(inetSocketAddress.getAddress()
                        .getHostAddress(), inetSocketAddress.getPort(), term);
                if (newChannel != null) {
                    List<Channel> slaveChannels;
                    if ((slaveChannels = termAndSlaveChannels.get(Long.toString(term))) != null) {
                        slaveChannels.add(newChannel);
                        addListenerOnChannel(newChannel, term);
                    }
                }
            }
        });
    }

    /**
     * 全量同步(日志、状态机等),用作主从失连恢复后数据同步
     */
    public static void fullSync(String committedLog, String uncommittedLog, byte[] dataObject, String coveredIndex) {
        LogService.syncCommittedLog(committedLog, coveredIndex);
        LogService.syncUncommittedLog(uncommittedLog);
        data = (Map<String, Set<String>>) DataSearialUtil.deserialize(dataObject).getObject();
    }


    /**
     * 获取当前角色
     */
    public static String getRole() {
        return role;
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
        return Integer.toString(COMMAND_ID_COUNTER.getAndIncrement());
    }


    /**
     * 增长当前任期
     */
    public static void increaseTerm() {
        term = LogService.increaseCurrentTerm(term);
    }

    /**
     * 更新当前任期
     * @return 原本的任期
     */
    public static long updateTerm(long term) {
        long preTerm = RaftNode.term;
        RaftNode.term = term;
        return preTerm;
    }


    /**
     * 启动raft节点
     */
    public static void start() {
        LogoUtil.printLogo();
        LOGGER.info("registry server started successfully");
    }

}
