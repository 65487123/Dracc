
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

import com.lzp.dracc.common.constant.Command;
import com.lzp.dracc.common.constant.Const;
import com.lzp.dracc.common.util.CommonUtil;
import com.lzp.dracc.common.util.PropertyUtil;
import com.lzp.dracc.common.util.StringUtil;
import com.lzp.dracc.common.util.ThreadFactoryImpl;
import com.lzp.dracc.server.netty.ConnectionFactory;
import com.lzp.dracc.server.netty.NettyServer;
import com.lzp.dracc.server.netty.CoreHandler;
import com.lzp.dracc.server.util.CountDownLatch;
import com.lzp.dracc.server.util.DataSearialUtil;
import com.lzp.dracc.server.util.LogoUtil;
import com.lzp.dracc.server.util.ThreadPoolExecutor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.json.GsonBuilderUtils;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import static java.nio.charset.StandardCharsets.UTF_8;

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
    private static class DelayTask implements Delayed, Runnable {

        Runnable runnable;
        volatile long deadline;

        DelayTask(Runnable runnable, long delay) {
            this.runnable = runnable;
            this.deadline = System.currentTimeMillis() + delay;
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
     * 执行定时任务(心跳、服务健康检查、锁健康检查)的线程池
     */
    private static ScheduledExecutorService heartBeatAndHealthCheckExecutor;

    /**
     * 执行重连任务的线程池
     */
    private static ExecutorService reconnectionExecutor;

    /**
     * 用来给客户端发送通知的线程池
     */
    private static final ExecutorService THREAD_POOL_FOR_NOTI = new ThreadPoolExecutor(1, 1, 0
            , new LinkedBlockingQueue(), new ThreadFactoryImpl("send notice"));

    /**
     * 用来延时删除锁的线程池
     */
    private static final ScheduledExecutorService THREAD_POOL_FOR_REL_LOCK = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("Release locks with a delay"));

    /**
     * 超时选举任务
     */
    private static final DelayTask ELECTION_TASK;

    /**
     * 任期
     */
    public static volatile long term;

    /**
     * 当前任期是否没投过票
     */
    public static volatile boolean notVoted = true;

    /**
     * 和客户端的连接,主节点才有元素
     */
    public static final Map<String, List<Channel>> IP_CHANNELS_WITH_CLIENT_MAP = new ConcurrentHashMap<>();

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
    public static Map<String, Object>[] data = new Map[3];

    /**
     * 所有将要被发送的通知(服务监听和锁的功能会用到)
     * key是客户端ip,value是向这个ip发送通知任务(一个服务名或者client端的请求id对应一个任务)的队列
     */
    public static final Map<String, BlockingQueue<String>> ALL_NOTIFICATION_TOBESENT = new ConcurrentHashMap<>();


    static {
        Properties clusterProperties = PropertyUtil.getProperties(Const.CLU_PRO);
        String localNode = clusterProperties.getProperty("localRaftNode");
        LOGGER.info("server:'{}' is starting", localNode);
        term = Long.parseLong(LogService.getTerm());
        String[] remoteNodeIps = clusterProperties.getProperty("peerRaftNodes").split(",");
        setReconnectionExecutor(remoteNodeIps.length);
        HALF_COUNT = (short) (remoteNodeIps.length % 2 == 0 ? remoteNodeIps.length / 2 : remoteNodeIps.length / 2 + 1);
        String[] localIpAndPort = StringUtil.stringSplit(localNode, Const.COLON);
        NettyServer.start(localIpAndPort[0], Integer.parseInt(localIpAndPort[1]));
        setThreadPoolForPerformElectTasks();
        timeoutToElectionExecutor.execute(ELECTION_TASK = new DelayTask(() -> {
            LOGGER.info("heartbeat timed out, initiate an election");
            startElection(remoteNodeIps);
        }, ThreadLocalRandom.current().nextInt(9500, 28500)));
        startThreadForNoti();
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
    private static void setThreadPoolForPerformElectTasks() {
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
        RaftNode.notVoted = false;
        updateTermAndSlaveChannels();
        String voteRequestId = Long.toString(term);
        CountDownLatch countDownLatch = new CountDownLatch(HALF_COUNT);
        cidAndResultMap.put(voteRequestId, countDownLatch);
        ExecutorService threadPoolExecutor = new ThreadPoolExecutor(remoteNodeIps.length, remoteNodeIps
                .length, 0, new LinkedBlockingQueue<>(), new ThreadFactoryImpl("ask for vote"));
        for (String remoteNodeIp : remoteNodeIps) {
            String[] ipAndPort = StringUtil.stringSplit(remoteNodeIp, Const.COLON);
            threadPoolExecutor.execute(() -> sendRpcAndSaveChannel(term, voteRequestId, ipAndPort[0], ipAndPort[1]));
        }
        try {
            if (countDownLatch.await(ThreadLocalRandom.current().nextLong(7000, 21000), TimeUnit.MILLISECONDS)) {
                upgradToLeader(Long.toString(term));
            } else {
                LOGGER.info("The election timed out, re-launch");
                startElection(remoteNodeIps);
            }
        } catch (InterruptedException e) {
            LOGGER.info("Interrupted while waiting for vote");
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
        if (oldChannels != null) {
            for (Channel channel : oldChannels) {
                channel.close();
            }
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
        heartBeatAndHealthCheckExecutor = new ScheduledThreadPoolExecutor(3
                , new ThreadFactoryImpl("heartbeatAndHealthCheck"));
        heartBeatAndHealthCheckExecutor.scheduleWithFixedDelay(RaftNode::heartbeatToSlaves, 5, 5, TimeUnit.SECONDS);
        heartBeatAndHealthCheckExecutor.scheduleWithFixedDelay(RaftNode::performServiceHealthCheck, 5, 20, TimeUnit.SECONDS);
        heartBeatAndHealthCheckExecutor.scheduleWithFixedDelay(RaftNode::performLockHealthCheck, 5, 40, TimeUnit.SECONDS);
        //放到io线程中执行是为了保证单线程模型
        NettyServer.workerGroup.execute(() -> {
            LogService.commitAllUncommittedLog();
            //防止原主挂了导致通知任务丢失,选举出新主后重新向所有已注册监听的客户端发送一遍监听的服务内容通知
            sentNotifications();
        });
    }


    /**
     * 向所有已注册监听的客户端发送一遍监听的服务内容通知
     */
    private static void sentNotifications() {
        for (String service : data[0].keySet()) {
            notifyListeners(service, "");
        }
    }


    /**
     * 向从节点发心跳
     */
    private static void heartbeatToSlaves() {
        byte[] emptyPackage = new byte[0];
        for (Channel channel : CoreHandler.slaves) {
            channel.writeAndFlush(emptyPackage);
        }
    }


    /**
     * 执行服务健康检查
     */
    private static void performServiceHealthCheck() {
        for (Map.Entry<String, Object> serviceAndInstances : data[0].entrySet()) {
            Set<String> instances;
            synchronized (instances = (Set<String>) serviceAndInstances.getValue()) {
                for (String instance : instances) {
                    if (!isAlive(instance)) {
                        /*当检查出存活的客户端中没有这个服务实例时会进到这里,执行下面这段代码:把删除服务实例的任务
                        丢进netty的io线程中执行(接收客户端业务请求的线程,单线程设计的)。
                        */
                        /*
                        当执行NioEventLoopGroup().execute(),netty底层最终会调用到NioEventLoop的
                        execute(),把任务塞进他的任务队列中。NioEventLoop在select()前会判断一次队列中
                        是否有任务,如果有任务会selectNow()然后先处理已就绪的io请求再执行这个任务。如果
                        NioEventLoop已经在阻塞select()了,会唤醒他并执行任务(如果execute()的runnable
                        实现了NonWakeupRunnable,则不会唤醒)
                        */
                        NettyServer.workerGroup.execute(() -> {
                            /*
                            如果刚进入到这里,客户端就已经重连上并且server端已经把客户端的连接加入到容器中
                            然后返回给客户端连接成功消息,客户端收到消息后马上重新把服务注册了一遍。
                            那么这里执行删除逻辑可能会有问题,所以这里再加一次判断。(但实际基本是不可能出现这种
                            情况的,因为从第一次判断到execute()方法执行完都是纯cpu计算,不能被中断的。而上面说的那
                            种情况至少得经历一个RTT(Round-Trip Time))
                            */
                            if (!isAlive(instance)) {
                                CoreHandler.handleServiceWrite(generCommandForDelService(Thread.currentThread()
                                        .getName() + serviceAndInstances.getKey() + instance, serviceAndInstances
                                        .getKey(), instance), null);
                            }
                        });
                    }
                }
            }
        }
    }

    /**
     * 执行锁健康检查
     */
    private static void performLockHealthCheck() {
        LinkedList<String> list;
        for (Map.Entry<String, Object> entry : data[2].entrySet()) {
            if ((list = (LinkedList<String>) entry.getValue()) != null) {
                synchronized (list) {
                    String[] lockHolder;
                    if (!list.isEmpty() && !isAlive((lockHolder = StringUtil.stringSplit(list
                            .getFirst(), Const.COLON))[0])) {
                        delayToReleaseLock(list, lockHolder, entry.getKey());
                    }
                }
            }
        }
    }

    /**
     * 延迟释放锁
     */
    private static void delayToReleaseLock(final LinkedList waiter, String[] lockHolder, String lockName) {
        THREAD_POOL_FOR_REL_LOCK.schedule(() -> {
            synchronized (waiter) {
                if (!waiter.isEmpty() && lockHolder.equals(waiter
                        .getFirst()) && !isAlive(lockHolder[0])) {
                    //释放锁是幂等操作
                    NettyServer.workerGroup.execute(() -> CoreHandler.handleLockWrite(new String[]{lockHolder[1],
                            Const.RPC_FROMCLIENT, Const.TWO + Const.COMMAND_SEPARATOR + Command.REM,
                            lockName, lockHolder[0]}, null));
                }
            }
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * 生成删除服务实例的命令
     */
    private static String[] generCommandForDelService(String commandName, String serviceName, String instance) {
        return new String[]{commandName, Const.RPC_FROMCLIENT, Const.ZERO
                , Command.REM, serviceName, instance};
    }

    /**
     * 查看实例对应的客户端是否存活
     */
    private static boolean isAlive(String instance) {
        List<Channel> channels;
        return (channels = IP_CHANNELS_WITH_CLIENT_MAP.get(StringUtil
                .stringSplit(instance, Const.COLON)[0])) != null &&
                !channels.isEmpty();
    }


    /**
     * 当主节点收到更高任期的消息时(网络分区恢复后)或者候选者发现已经有leader了,
     * 或者遇到其他特殊情况,为了防止出现数据不一致,
     * 会执行此方法,降级为从节点
     * <p>
     * 这是个幂等操作
     *
     * @param newTerm 新任期
     */
    public synchronized static void downgradeToSlaveNode(boolean needClearUncommitLog, long newTerm) {
        LOGGER.info("downgrade to slave node");
        RaftNode.notVoted = true;
        long preTerm = RaftNode.updateTerm(term, newTerm);
        List<Channel> oldChannels = TERM_AND_SLAVECHANNELS.remove(Long.toString(preTerm));
        if (oldChannels != null) {
            for (Channel channel : oldChannels) {
                channel.close();
            }
        }
        role = Role.FOLLOWER;
        clearChannelsWithClient();
        shutdownHeartbeatExecutor();
        CoreHandler.resetReplicationThreadPool();
        if (needClearUncommitLog) {
            LogService.clearUncommittedEntry();
        }
        timeoutToElectionExecutor.shutdownNow();
        setThreadPoolForPerformElectTasks();
        resetTimer();
        timeoutToElectionExecutor.execute(ELECTION_TASK);
    }


    /**
     * 关闭执行心跳任务的线程池
     */
    private static void shutdownHeartbeatExecutor() {
        if (heartBeatAndHealthCheckExecutor != null) {
            heartBeatAndHealthCheckExecutor.shutdownNow();
        }
    }

    /**
     * 关闭所有与客户端的连接,并从容器中清除
     */
    private static void clearChannelsWithClient() {
        for (Map.Entry<String, List<Channel>> entry : IP_CHANNELS_WITH_CLIENT_MAP.entrySet()) {
            List<Channel> channels = entry.getValue();
            for (int i = channels.size() - 1; i >= 0; i--) {
                channels.remove(i).close();
            }
        }
        IP_CHANNELS_WITH_CLIENT_MAP.clear();
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
        data = (Map<String, Object>[]) DataSearialUtil.deserialize(dataObject).getObject();
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
        ELECTION_TASK.deadline = System.currentTimeMillis() + ThreadLocalRandom.current()
                .nextInt(9500, 28500);
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
     * 启动执行发通知任务的线程
     */
    private static void startThreadForNoti() {
        THREAD_POOL_FOR_NOTI.execute(() -> {
            for (; ; ) {
                try {
                    if (role == Role.LEADER) {
                        BlockingQueue<String> queue;
                        String task;
                        for (Map.Entry<String, List<Channel>> entry : IP_CHANNELS_WITH_CLIENT_MAP.entrySet()) {
                            if (!entry.getValue().isEmpty()) {
                                if ((queue = ALL_NOTIFICATION_TOBESENT.get(entry.getKey())) != null) {
                                    while ((task = queue.poll()) != null) {
                                        if (task.startsWith("lockWaiter:")) {
                                            for (Channel channel : entry.getValue()) {
                                                wakeUpLockWaiter(channel, StringUtil.stringSplit(task, Const.COLON)[1]);
                                            }
                                        } else {
                                            for (Channel channel : entry.getValue()) {
                                                sentNotification(channel, task);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Thread.sleep(100);
                } catch (Exception ignored) {
                }
            }
        });
    }

    /**
     * 唤醒等待锁的客户端线程
     */
    private static void wakeUpLockWaiter(Channel channel, String commandId) {
        channel.writeAndFlush((commandId + Const.COMMA + Const.TRUE).getBytes(UTF_8));
    }

    /**
     * 向客户端发送通知
     */
    private static void sentNotification(Channel channel, String service) {
        channel.writeAndFlush((Const.UUID + Const.COMMA + service + Const
                .SPECIFICORDER_SEPARATOR + CommonUtil.serial((Set<String>) RaftNode
                .data[0].get(service))).getBytes(UTF_8));
    }


    /**
     * 通知所有监听器
     */
    public static void notifyListeners(String serviceName, String excludedIp) {
        Set<String> ips;
        if ((ips = (Set<String>) RaftNode.data[1].get(serviceName)) != null) {
            for (String ip : ips) {
                if (!ip.equals(excludedIp)) {
                    BlockingQueue<String> queue;
                    if ((queue = ALL_NOTIFICATION_TOBESENT.get(ip)) == null) {
                        queue = new LinkedBlockingQueue<>();
                        ALL_NOTIFICATION_TOBESENT.put(ip, queue);
                    }
                    queue.offer(serviceName);
                }
            }
        }
    }


    /**
     * 对方日志不比自己旧就投他一票
     */
    public static boolean voteIfNotVotedAndTheLogMatches(String oppoCommittedLogIndex, String oppoUnCommittedLogIndex,
                                                         String reqId, ChannelHandlerContext channelHandlerContext) {
        if (RaftNode.notVoted && logIsNotOlder(oppoCommittedLogIndex, oppoUnCommittedLogIndex)) {
            channelHandlerContext.writeAndFlush((reqId + Const.COMMA + Const.YES).getBytes(UTF_8));
            RaftNode.notVoted = false;
            LOGGER.info("Voted for the candidate : {}", ((InetSocketAddress) channelHandlerContext
                    .channel().remoteAddress()).getAddress().getHostAddress());
            return true;
        }
        return false;
    }


    /**
     * 判断日志是否不比本节点日志旧
     */
    public static boolean logIsNotOlder(String commiteedLogIndex, String uncommiteedLogIndex) {
        return Long.parseLong(commiteedLogIndex) >= LogService.getCommittedLogIndex()
                && Long.parseLong(uncommiteedLogIndex) >= LogService.getUncommittedLogSize();
    }

    /**
     * 启动raft节点
     */
    public static void start() {
        LogoUtil.printLogo();
        LOGGER.info("dracc server started successfully");
    }

}
