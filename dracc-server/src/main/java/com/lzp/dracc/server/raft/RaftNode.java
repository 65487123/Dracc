
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

package com.lzp.dracc.server.raft;

import com.lzp.dracc.common.constant.Const;
import com.lzp.dracc.common.util.PropertyUtil;
import com.lzp.dracc.common.util.ThreadFactoryImpl;
import com.lzp.dracc.server.netty.ConnectionFactory;
import com.lzp.dracc.server.netty.NettyServer;
import com.lzp.dracc.server.netty.CoreHandler;
import com.lzp.dracc.server.util.CountDownLatch;
import com.lzp.dracc.server.util.DataSearialUtil;
import com.lzp.dracc.server.util.LogoUtil;
import com.lzp.dracc.server.util.ThreadPoolExecutor;
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
        volatile long deadline;
        volatile long delay;

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
    private static volatile Role role = Role.FOLLOWER;

    /**
     * 执行超时选举任务的线程池
     */
    private static ExecutorService timeoutToElectionExecutor;

    /**
     * 执行心跳任务的线程池
     */
    private static ExecutorService heartBeatExecutor;

    /**
     * 执行重连任务的线程池
     */
    private static ExecutorService reconnectionExecutor;

    /**
     * 超时选举任务
     */
    private static DelayTask electionTask;

    /**
     * 任期
     */
    public static volatile long term;

    /**
     * 和客户端的连接,主节点才有元素
     */
    public static final List<Channel> CHANNELS_WITH_CLIENT = new ArrayList<>();

    /**
     * 任期以及和从节点的连接,主节点才有元素
     */
    public static final Map<String, List<Channel>> TERM_AND_SLAVECHANNELS = new ConcurrentHashMap<>();

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
    public static Map<String, Set<String>>[] data = new HashMap[2];

    static {
        Properties clusterProperties = PropertyUtil.getProperties(Const.CLU_PRO);
        String localNode = clusterProperties.getProperty("localRaftNode");
        LOGGER.info("server:'{}' is starting", localNode);
        term = Long.parseLong(LogService.getTerm());
        String[] remoteNodeIps = clusterProperties.getProperty("peerRaftNodes").split(Const.COMMA);
        setReconnectionExecutor(remoteNodeIps.length);
        HALF_COUNT = (short) (remoteNodeIps.length % 2 == 0 ? remoteNodeIps.length / 2 : remoteNodeIps.length / 2 + 1);
        String[] localIpAndPort = localNode.split(Const.COLON);
        NettyServer.start(localIpAndPort[0], Integer.parseInt(localIpAndPort[1]));
        setThreadPoolForPerformElectTasks();
        electionTask = new DelayTask(() -> {
            LOGGER.info("heartbeat timed out, initiate an election");
            startElection(remoteNodeIps);
        }, ThreadLocalRandom.current().nextInt(12000, 18000));
        timeoutToElectionExecutor.execute(electionTask);
    }


    /**
     * 设置执行重连任务的线程池
     */
    private static void setReconnectionExecutor(int maxNum) {
        reconnectionExecutor = new ThreadPoolExecutor(0, maxNum, 0, new LinkedBlockingQueue<Runnable>() {
            @Override
            public boolean offer(Runnable runnable) {
                return false;
            }
        }, new ThreadFactoryImpl("reconnection"));
    }

    /**
     * 重置执行选举任务的线程池
     */
    private static void setThreadPoolForPerformElectTasks(){
        timeoutToElectionExecutor = new ThreadPoolExecutor(1, 1, 0, new DelayQueue(),
                new ThreadFactoryImpl("timeout to election"));
        timeoutToElectionExecutor.execute(() -> {
        });
    }

    /**
     * 发起选举
     */
    private static void startElection(String[] remoteNodeIps) {
        role = Role.CANDIDATE;
        updateTermAndSlaveChannels();
        String voteRequestId = Long.toString(term);
        CountDownLatch countDownLatch = new CountDownLatch(HALF_COUNT);
        cidAndResultMap.put(voteRequestId, countDownLatch);
        ExecutorService threadPoolExecutor = new ThreadPoolExecutor(remoteNodeIps.length, remoteNodeIps
                .length, 0, new LinkedBlockingQueue<>(), new ThreadFactoryImpl("ask for vote"));
        for (String remoteNodeIp : remoteNodeIps) {
            String[] ipAndPort = remoteNodeIp.split(Const.COLON);
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
        Channel channel = ConnectionFactory.newChannelAndRequestForVote(voteRequestId, ip, Integer
                .parseInt(port), currentTerm, LogService.getCommittedLogIndex(), LogService.getUncommittedLogSize());
        if (channel != null) {
            List<Channel> channelsToSlave;
            if ((channelsToSlave = TERM_AND_SLAVECHANNELS.get(Long.toString(currentTerm))) != null) {
                channelsToSlave.add(channel);
                addListenerOnChannel(channel, term);
            }
        }
    }


    /**
     * 断开旧连接并从容器中移除,增加装新连接的容器,发起选举时会执行此方法
     */
    private static void updateTermAndSlaveChannels() {
        List<Channel> oldChannels = TERM_AND_SLAVECHANNELS.remove(Long.toString(term));
        for (Channel channel : oldChannels) {
            channel.close();
        }
        increaseTerm();
        TERM_AND_SLAVECHANNELS.put(Long.toString(term), new CopyOnWriteArrayList<>());
    }


    /**
     * 升级成主节点
     */
    private static void upgradToLeader(String term) {
        LOGGER.info("successful election, upgrade to the master node");
        role = Role.LEADER;
        CoreHandler.slaves = TERM_AND_SLAVECHANNELS.get(term);
        heartBeatExecutor = new ThreadPoolExecutor(1, 1, 0, new LinkedBlockingQueue<>(),
                new ThreadFactoryImpl("heatbeat"));
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
     * 或者遇到其他特殊情况,为了防止出现数据不一致,
     * 会执行此方法,降级为从节点
     *
     * @param newTerm 新任期
     */
    public static void downgradeToSlaveNode(boolean needClearUncommitLog, long newTerm) {
        LOGGER.info("downgrade to slave node");
        long preTerm = RaftNode.updateTerm(term, newTerm);
        List<Channel> oldChannels = TERM_AND_SLAVECHANNELS.remove(Long.toString(preTerm));
        for (Channel channel : oldChannels) {
            channel.close();
        }
        role = Role.FOLLOWER;
        clearChannelsWithClient();
        heartBeatExecutor.shutdownNow();
        CoreHandler.resetReplicationThreadPool();
        if (needClearUncommitLog) {
            LogService.clearUncommittedEntry();
        }
        timeoutToElectionExecutor.shutdownNow();
        setThreadPoolForPerformElectTasks();
        resetTimer();
        timeoutToElectionExecutor.execute(electionTask);
    }


    /**
     * 关闭所有与客户端的连接,并从容器中清除
     */
    private static void clearChannelsWithClient() {
        synchronized (CHANNELS_WITH_CLIENT) {
            for (int i = CHANNELS_WITH_CLIENT.size() - 1; i >= 0; i--) {
                CHANNELS_WITH_CLIENT.remove(i).close();
            }
        }
    }

    /**
     * 监听channel,当channel关闭后,根据具体情况选择是否重连
     */
    private static void addListenerOnChannel(Channel channel, long term) {
        //当连接断开后,channel.remoteAddress()会得到null,所以这里需要提前获取
        InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.remoteAddress();
        channel.closeFuture().addListener(future -> {
            reconnectionExecutor.execute(() -> {
                List<Channel> slaveChannels;
                if ((slaveChannels = TERM_AND_SLAVECHANNELS.get(Long.toString(term))) != null) {
                    //说明不是主动断开连接的(降级为从节点或者重新选举)
                    slaveChannels.remove(channel);
                    Channel newChannel = ConnectionFactory.newChannelAndAskForSync(inetSocketAddress.getAddress()
                            .getHostAddress(), inetSocketAddress.getPort(), term);
                    if (newChannel != null) {
                        //再次判断存当前任期从节点的容器是否还在,如果不在了,说明任期已经更新或者自己已经不是主节点了
                        if (TERM_AND_SLAVECHANNELS.get(Long.toString(term)) != null) {
                            slaveChannels.add(newChannel);
                            addListenerOnChannel(newChannel, term);
                        } else {
                            newChannel.close();
                        }
                    }
                }
            });
        });
    }

    /**
     * 全量同步(日志、状态机等),用作主从失连恢复后数据同步
     */
    public static void fullSync(String committedLog, String uncommittedLog, byte[] dataObject, String coveredIndex) {
        LogService.syncCommittedLog(committedLog, coveredIndex);
        LogService.syncUncommittedLog(uncommittedLog);
        data = (Map<String, Set<String>>[]) DataSearialUtil.deserialize(dataObject).getObject();
    }


    /**
     * 获取当前角色
     */
    public static Role getRole() {
        return role;
    }


    /**
     * 重置计时器
     */
    public static void resetTimer() {
        electionTask.deadline = System.currentTimeMillis() + electionTask.delay;
    }


    /**
     * 增长当前任期,只有执行选举任务时会调用
     */
    public static synchronized void increaseTerm() {
        updateTerm(term, term + 1);
    }

    /**
     * 更新当前任期
     *
     * @return 原本的任期
     */
    public static synchronized long updateTerm(long preTerm, long newTerm) {
        if (preTerm != term) {
            //进到这里,说明执行了心跳超时选举任务,需要终止
            downgradeToSlaveNode(false, newTerm);
        } else {
            LogService.updateCurrentTerm(Long.toString(newTerm));
            RaftNode.term = newTerm;
        }
        return preTerm;
    }


    /**
     * 启动raft节点
     */
    public static void start() {
        LogoUtil.printLogo();
        LOGGER.info("dracc server started successfully");
    }

}
