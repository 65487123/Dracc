
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

package com.lzp.dracc.server.netty;

import com.lzp.dracc.common.constant.Command;
import com.lzp.dracc.common.constant.Const;
import com.lzp.dracc.common.util.CommonUtil;
import com.lzp.dracc.common.util.StringUtil;
import com.lzp.dracc.common.util.ThreadFactoryImpl;
import com.lzp.dracc.server.raft.LogService;
import com.lzp.dracc.server.raft.RaftNode;
import com.lzp.dracc.server.raft.Role;
import com.lzp.dracc.server.util.ConcurrentArrayList;
import com.lzp.dracc.server.util.CountDownLatch;
import com.lzp.dracc.server.util.ThreadPoolExecutor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Description:处理rpc请求核心handler
 *
 * @author: Zeping Lu
 * @date: 2021/3/19 10:56
 */
public class CoreHandler extends SimpleChannelInboundHandler<byte[]> {

    private final static Logger LOGGER = LoggerFactory.getLogger(CoreHandler.class);

    /**
     * 日志复制时执行等待从节点响应并做出最终决定任务的单线程线程池
     */
    private static ExecutorService repilicationThreadPool;

    /**
     * 和所有从节点的连接
     */
    public static List<Channel> slaves;

    /**
     * 给从节点用的单线程线程池。当日志复制时并且日志不同步(不连续)的时候会用到
     */
    private final ExecutorService SINGLE_THREAD_POOL = new ThreadPoolExecutor(1, 1, 0
            , new LinkedBlockingQueue(), new ThreadFactoryImpl("replication thread when log not synced"));


    /**
     * 单线程操作(一个io线程),无需加volatile
     */
    private boolean logMustBeConsistent = false;

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        SINGLE_THREAD_POOL.shutdownNow();
    }

    static {
        repilicationThreadPool = new ThreadPoolExecutor(1, 1, 0, new ArrayBlockingQueue<>(1000),
                new ThreadFactoryImpl("replication thread"), (r, executor) -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
            executor.execute(r);
        });
    }

    public static void resetReplicationThreadPool() {
        repilicationThreadPool.shutdownNow();
        repilicationThreadPool = new ThreadPoolExecutor(1, 1, 0, new ArrayBlockingQueue<Runnable>(1000),
                new ThreadFactoryImpl("replication thread"), (r, executor) -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
            executor.execute(r);
        });
    }

    /**
     * 把命令根据分隔符分割获得一个字符串数组
     * 数组第一个位置就是这个请求的请求id,第二个位置就是具体的请求类型
     * <p>
     * 如果这个节点是主节点,
     * 第三个位置表示是服务还是配置,第四个位置是读写请求的具体行为(add、get等),第五个位置是key,第六个位置是value,
     */
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, byte[] bytes) {
        String[] command = StringUtil.stringSplit(new String(bytes, UTF_8), Const.COMMAND_SEPARATOR);
        //分支不是特别多的情况下,if/else性能比switch要高,尤其是把高频率的分支放在前面
        if (Const.RPC_FROMCLIENT.equals(command[1])) {
            handleClientReq(command, channelHandlerContext);
        } else if (Const.RPC_REPLICATION.equals(command[1])) {
            handleReplicationReq(command, channelHandlerContext);
        } else if (Const.RPC_COMMIT.equals(command[1])) {
            LogService.commitFirstUncommittedLog();
        } else if (Const.RPC_SYNC_TERM.equals(command[1])) {
            handleSyncTermReq(Long.parseLong(command[2]), channelHandlerContext);
        } else if (Const.RPC_ASKFORVOTE.equals(command[1])) {
            voteIfAppropriate(channelHandlerContext, command);
        } else if (Const.RPC_GETROLE.equals(command[1])) {
            handleGetRole(command, channelHandlerContext);
        } else {
            //Const.COPY_LOG_REPLY.equals(command[1])
            syncLogAndStateMachine(command);
        }
    }

    private void handleGetRole(String[] command,ChannelHandlerContext channelHandlerContext) {
        if (RaftNode.getRole() == Role.LEADER) {
            Channel channel = channelHandlerContext.channel();
            String remoteIp = ((InetSocketAddress) channel.remoteAddress())
                    .getAddress().getHostAddress();
            List<Channel> channels;
            if ((channels = RaftNode.IP_CHANNELS_WITH_CLIENT_MAP.get(remoteIp)) == null) {
                channels = new ConcurrentArrayList<>();
                RaftNode.IP_CHANNELS_WITH_CLIENT_MAP.put(remoteIp, channels);
            }
            channels.add(channel);
            List<Channel> finalChannels = channels;
            channel.closeFuture().addListener(future -> {
                finalChannels.remove(channel);
                if (finalChannels.isEmpty()) {
                    RaftNode.IP_CHANNELS_WITH_CLIENT_MAP.remove(remoteIp);
                }
            });
        }
        channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + RaftNode.getRole().name()).getBytes(UTF_8));
    }

    /**
     * 同步日志以及状态机
     */
    private synchronized void syncLogAndStateMachine(String[] command) {
        //0表示是全量同步
        if (Const.ZERO.equals(command[1])) {
            RaftNode.fullSync(command[2], command[3], command[4].getBytes(UTF_8), command[5]);
        } else {
            LogService.syncUncommittedLog(command[3]);
        }
        logMustBeConsistent = true;
        this.notify();
    }


    /**
     * 处理日志复制请求
     */
    private void handleReplicationReq(String[] command, ChannelHandlerContext channelHandlerContext) {
        /*
        收到这个消息时,任期肯定是已经同步了(建连接时会同步),
        所以,只需要判断新添加的日志索引是否能接上上一个索引就行
        如果不能接上,则需要先同步主节点的所有日志再复制这一条日志
        */
        if (logMustBeConsistent) {
            appendUncommittedLogAndReturnYes(command, channelHandlerContext);
        } else if (Long.parseLong(command[3]) == LogService.getCommittedLogIndex() && Long
                .parseLong(command[4]) - 1 == LogService.getUncommittedLogSize()) {
            logMustBeConsistent = true;
            appendUncommittedLogAndReturnYes(command, channelHandlerContext);
        } else {
            SINGLE_THREAD_POOL.execute(() -> waitUntilSyncThenReturnYes(command, channelHandlerContext));
        }
    }

    /**
     * 添加日志并且返回成功复制消息
     */
    private void appendUncommittedLogAndReturnYes(String[] command, ChannelHandlerContext channelHandlerContext) {
        LogService.appendUnCommittedLog(command[2]);
        channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + Const.YES).getBytes(UTF_8));
    }

    /**
     * 等待同步日志成功,然后回日志复制成功结果
     */
    private synchronized void waitUntilSyncThenReturnYes(String[] command, ChannelHandlerContext channelHandlerContext) {
        if (!logMustBeConsistent) {
            channelHandlerContext.writeAndFlush((Const.COPY_LOG_REQ + Const.COMMA + LogService
                    .getCommittedLogIndex()).getBytes(UTF_8));
            while (!logMustBeConsistent) {
                try {
                    this.wait();
                } catch (InterruptedException ignored) {
                }
            }
        }
        channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + Const.YES).getBytes(UTF_8));
    }

    /**
     * 收到投票请求,如果合适就投对方一票
     */
    private void voteIfAppropriate(ChannelHandlerContext channelHandlerContext, String[] command) {
        long opposingTerm = Long.parseLong(command[2]);
        if (Role.FOLLOWER == RaftNode.getRole()) {
            if (opposingTerm >= RaftNode.term) {
                RaftNode.updateTerm(RaftNode.term, opposingTerm);
                if (voteIfNotVotedAndTheLogMatches(command[3], command[4], command[0], channelHandlerContext)) {
                    RaftNode.resetTimer();
                }
            }
        } else if (Role.LEADER == RaftNode.getRole()) {
            if (opposingTerm > RaftNode.term) {
                //1、网络分区恢复后 2、对方和本节点同时发起选举,但是对方竞选失败发起下一轮选举
                RaftNode.downgradeToSlaveNode(false, opposingTerm);
                voteIfNotVotedAndTheLogMatches(command[3], command[4], command[0], channelHandlerContext);
            } else {
                //比对方先取得半数票,已经竞选成功或者网络分区后本节点在多数派
                channelHandlerContext.writeAndFlush((Const.RPC_TOBESLAVE + Const.COMMA + LogService.getTerm()).getBytes(UTF_8));
            }
        } else {
            if (opposingTerm > RaftNode.term) {
                RaftNode.downgradeToSlaveNode(false, opposingTerm);
                if (voteIfNotVotedAndTheLogMatches(command[3], command[4], command[0], channelHandlerContext)) {
                    RaftNode.downgradeToSlaveNode(false, opposingTerm);
                }
            }
        }
    }


    /**
     * 对方日志不比自己旧就投他一票
     */
    private boolean voteIfNotVotedAndTheLogMatches(String oppoCommittedLogIndex, String oppoUnCommittedLogIndex,
                                                   String reqId, ChannelHandlerContext channelHandlerContext) {

        if (RaftNode.notVoted && logIsNotOlder(oppoCommittedLogIndex, oppoUnCommittedLogIndex)) {
            channelHandlerContext.writeAndFlush((reqId + Const.COMMA + Const.YES).getBytes(UTF_8));
            RaftNode.notVoted = false;
            return true;
        }
        return false;
    }


    /**
     * 判断日志是否不比本节点日志旧
     */
    private boolean logIsNotOlder(String commiteedLogIndex, String uncommiteedLogIndex) {
        return Long.parseLong(commiteedLogIndex) >= LogService.getCommittedLogIndex()
                && Long.parseLong(uncommiteedLogIndex) >= LogService.getUncommittedLogSize();
    }

    /**
     * 判断日志是否比自己新
     */
    private boolean logIsNewer(String commiteedLogIndex, String uncommiteedLogIndex) {
        return Long.parseLong(commiteedLogIndex) > LogService.getCommittedLogIndex()
                && Long.parseLong(uncommiteedLogIndex) > LogService.getUncommittedLogSize();
    }

    /**
     * 处理客户端具体请求
     * 0：请求id  1:请求类型 2:服务、配置还是锁(0、1、2) 3:具体行为(add、get等) 4:key 5:value
     */
    private void handleClientReq(String[] command, ChannelHandlerContext channelHandlerContext) {
        //当少于半数节点存活,整个集群是不可用的,直接返回异常
        if (RaftNode.TERM_AND_SLAVECHANNELS.get(String.valueOf(RaftNode.term)).size() < RaftNode.HALF_COUNT) {
            channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + Const.EXCEPTION + Const
                    .CLUSTER_DOWN_MESSAGE).getBytes(UTF_8));
        } else {
            boolean isRem = Command.REM.equals(command[3]);
            if (Const.ZERO.equals(command[2])) {
                //service
                if (Command.GET.equals(command[3])) {
                    channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + CommonUtil
                            .serial((Set<String>) RaftNode.data[0].get(command[4]))).getBytes(UTF_8));
                } else {
                    handleServiceWrite(isRem, command, channelHandlerContext);
                }
            } else if (Const.ONE.equals(command[2])) {
                //config
                if (Command.GET.equals(command[3])) {
                    channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + CommonUtil
                            .serial((Set<String>) RaftNode.data[1].get(command[4]))).getBytes(UTF_8));
                } else {
                    handleConfigWrite(isRem, command, channelHandlerContext);
                }
            } else {
                //lock
                handleLockWrite(isRem, command, channelHandlerContext);
            }
        }
    }

    /**
     * 处理写服务请求
     */
    public static void handleServiceWrite(boolean isRem, String[] command, ChannelHandlerContext channelHandlerContext) {
        CountDownLatch countDownLatch = new CountDownLatch(RaftNode.HALF_COUNT);
        setLatchAndSendLogToSlaves(command, countDownLatch);
        repilicationThreadPool.execute(() -> receiveResponseForSvsAndMakeDecision(countDownLatch, command, channelHandlerContext, isRem));
    }


    /**
     * 把计数器放进容器,然后往从节点发送具体的命令日志
     */
    private static void setLatchAndSendLogToSlaves(String[] command, CountDownLatch countDownLatch) {
        RaftNode.cidAndResultMap.put(command[0], countDownLatch);
        //服务、配置、还是锁(0、1、2)、具体操作类型(remove、add)、key、value
        String specificOrder = command[2] + Const.SPECIFICORDER_SEPARATOR + command[3] + Const
                .SPECIFICORDER_SEPARATOR + command[4] + Const.SPECIFICORDER_SEPARATOR + command[5];
        long unCommittedLogNum = LogService.appendUnCommittedLog(specificOrder);
        for (Channel channel : slaves) {
            channel.writeAndFlush((command[0] + Const.COMMAND_SEPARATOR + Const.RPC_REPLICATION + Const
                    .COMMAND_SEPARATOR + specificOrder + Const.COMMAND_SEPARATOR + LogService.getCommittedLogIndex()
                    + Const.COMMAND_SEPARATOR + unCommittedLogNum).getBytes(StandardCharsets.UTF_8));
        }
    }


    /**
     * 处理写配置请求
     */
    public static void handleConfigWrite(boolean isRem, String[] command, ChannelHandlerContext channelHandlerContext) {
        CountDownLatch countDownLatch = new CountDownLatch(RaftNode.HALF_COUNT);
        setLatchAndSendLogToSlaves(command, countDownLatch);
        repilicationThreadPool.execute(() -> receiveResponseForConfAndMakeDecision(countDownLatch, command, channelHandlerContext, isRem));
    }

    /**
     * 处理获取释放锁请求
     */
    public static void handleLockWrite(boolean isRem, String[] command, ChannelHandlerContext channelHandlerContext) {
        CountDownLatch countDownLatch = new CountDownLatch(RaftNode.HALF_COUNT);
        command[5] = command[5] + Const.COLON + command[0];
        setLatchAndSendLogToSlaves(command, countDownLatch);
        repilicationThreadPool.execute(() -> receiveResponseForLocAndMakeDecision(countDownLatch, command, channelHandlerContext, isRem));
    }


    /**
     * 等待从节点的响应,并根据具体响应结果做出最终的决定
     */
    private static void receiveResponseForSvsAndMakeDecision(CountDownLatch countDownLatch, String[] command,
                                                             ChannelHandlerContext channelHandlerContext, boolean isRem) {
        boolean halfAgree = false;
        try {
            System.out.println("等待响应:"+ Arrays.toString(command));
            halfAgree = countDownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("interrupted when waitting for response from slaves", e);
        }
        if (halfAgree) {
            NettyServer.workerGroup.execute(() -> commitServiceLogAndReturnResult(command, channelHandlerContext, isRem));
        } else {
            //发送日志复制消息前有半数存活,发送日志复制消息时,却有连接断开了,或者是等待从节点响应过程中线程被中断了,
            // 防止数据不一致,重新选主
            NettyServer.workerGroup.execute(() -> RaftNode.downgradeToSlaveNode(false, RaftNode.term));
        }
    }

    /**
     * 等待从节点的响应,并根据具体响应结果做出最终的决定
     */
    private static void receiveResponseForConfAndMakeDecision(CountDownLatch countDownLatch, String[] command,
                                                              ChannelHandlerContext channelHandlerContext, boolean isRem) {
        boolean halfAgree = false;
        try {
            halfAgree = countDownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("interrupted when waitting for response from slaves", e);
        }
        if (halfAgree) {
            NettyServer.workerGroup.execute(() -> commitConfigLogAndReturnResult(command, channelHandlerContext, isRem));
        } else {
            //发送日志复制消息前有半数存活,发送日志复制消息时,却有连接断开了,或者是等待从节点响应过程中线程被中断了,
            // 防止数据不一致,重新选主
            NettyServer.workerGroup.execute(() -> RaftNode.downgradeToSlaveNode(false, RaftNode.term));
        }
    }

    /**
     * 等待从节点的响应,并根据具体响应结果做出最终的决定
     */
    private static void receiveResponseForLocAndMakeDecision(CountDownLatch countDownLatch, String[] command,
                                                             ChannelHandlerContext channelHandlerContext, boolean isRem) {
        boolean halfAgree = false;
        try {
            halfAgree = countDownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("interrupted when waitting for response from slaves", e);
        }
        if (halfAgree) {
            NettyServer.workerGroup.execute(() -> commitLockLogAndReturnResult(command, channelHandlerContext, isRem));
        } else {
            //发送日志复制消息前有半数存活,发送日志复制消息时,却有连接断开了,或者是等待从节点响应过程中线程被中断了,
            // 防止数据不一致,重新选主
            NettyServer.workerGroup.execute(() -> RaftNode.downgradeToSlaveNode(false, RaftNode.term));
        }
    }

    /**
     * 提交写服务日志、更新状态机并返回客户端结果
     */
    private static void commitServiceLogAndReturnResult(String[] command, ChannelHandlerContext channelHandlerContext, boolean isRem) {
        LogService.commitFirstUncommittedLog();
        Set<String> services;
        boolean operSucceed = false;
        if (isRem) {
            if ((services = (Set<String>) RaftNode.data[0].get(command[4])) != null) {
                //这里加锁是为了防止健康检查的线程读数据时出问题。写的时候都是单线程的(IO线程)。下面原因相同
                synchronized (services) {
                    operSucceed = services.remove(command[5]);
                }
                if (services.isEmpty()) {
                    RaftNode.data[0].remove(command[4]);
                }
            }
        } else {
            if ((services = (Set<String>) RaftNode.data[0].get(command[4])) == null) {
                services = new HashSet<>();
                RaftNode.data[0].put(command[4], services);
            }
            synchronized (services) {
                operSucceed = services.add(command[5]);
            }
        }
        //channelHandlerContext有可能为null(健康检查时删除服务会传null)。
        if (channelHandlerContext != null) {
            channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + operSucceed).getBytes(UTF_8));
        }
        notifySlavesToCommitTheLog();
        if (operSucceed) {
            RaftNode.notifyListeners(command[4]);
        }
    }

    /**
     * 通知从节点提交日志
     */
    private static void notifySlavesToCommitTheLog(){
        for (Channel channel : slaves) {
            channel.writeAndFlush(("x" + Const.COMMAND_SEPARATOR + Const
                    .RPC_COMMIT + Const.COMMAND_SEPARATOR).getBytes(UTF_8));
        }
    }

    /**
     * 提交写配置日志、更新状态机并返回客户端结果(适用于写服务和写配置)
     */
    private static void commitConfigLogAndReturnResult(String[] command, ChannelHandlerContext channelHandlerContext, boolean isRem) {
        LogService.commitFirstUncommittedLog();
        Set<String> configs;
        if (isRem) {
            if ((configs = (Set<String>) RaftNode.data[1].get(command[4])) != null) {
                channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + configs.remove(command[5])).getBytes(UTF_8));
            } else {
                channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + Const.FALSE).getBytes(UTF_8));
            }
        } else {
            if ((configs = (Set<String>) RaftNode.data[1].get(command[4])) == null) {
                configs = new HashSet<>();
                RaftNode.data[1].put(command[4], configs);
            }
            channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + configs.add(command[5])).getBytes(UTF_8));
        }
        notifySlavesToCommitTheLog();
    }

    /**
     * 提交写锁日志、更新状态机并返回客户端结果(适用于写服务和写配置)
     */
    private static void commitLockLogAndReturnResult(String[] command, ChannelHandlerContext channelHandlerContext, boolean isRem) {
        LogService.commitFirstUncommittedLog();
        if (isRem) {
            releaseLock(command, channelHandlerContext);
        } else {
            acquireLock(command, channelHandlerContext);
        }
        notifySlavesToCommitTheLog();
    }

    /**
     * 获取分布式锁逻辑
     */
    private static void acquireLock(String[] command, ChannelHandlerContext channelHandlerContext) {
        List<String> locks;
        int index;
        if ((locks = (List<String>) RaftNode.data[2].get(command[4])) == null) {
            locks = new LinkedList<>();
            RaftNode.data[2].put(command[4], locks);
            locks.add(command[5]);
            channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + Const.TRUE).getBytes(UTF_8));
        } else if (locks.size() == 0) {
            synchronized (locks) {
                //这里加锁是为了防止健康检查的线程读数据时出问题。写的时候都是单线程的(IO线程)。
                locks.add(command[5]);
            }
            channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + Const.TRUE).getBytes(UTF_8));
        } else if ((index = locks.indexOf(command[5])) == -1) {
            locks.add(command[5]);
        } else if (index == 0) {
            channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + Const.TRUE).getBytes(UTF_8));
        }
    }


    /**
     * 释放分布式锁逻辑
     */
    private static void releaseLock(String[] command, ChannelHandlerContext channelHandlerContext) {
        try {
            LinkedList<String> locks;
            //只有当前持有锁才有释放锁的权力
            if ((locks = (LinkedList<String>) RaftNode.data[2].get(command[3])) != null && command[4].equals(locks.getFirst())) {
                //这里加锁是为了防止健康检查的线程读数据时出问题。写的时候都是单线程的(IO线程)。
                synchronized (locks) {
                    locks.removeFirst();
                }
                channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + Const.TRUE).getBytes(UTF_8));
                if (locks.size() > 0) {
                    wakeUpWaiter(locks.getFirst());
                }
            } else {
                channelHandlerContext.writeAndFlush((command[0] + Const.COMMA + Const.FALSE).getBytes(UTF_8));
            }
        } catch (NullPointerException ignored) {
            //健康检查出持有锁的客户端已经失连了一段时间,释放这把锁时channelHandlerContext会传null,用try catch是为了不影响正常情况性能
        }
    }

    /**
     * 唤醒等待锁释放队列中的第一个
     */
    private static void wakeUpWaiter(String waiter) {
        String[] ipAndCommandId = StringUtil.stringSplit(waiter, Const.COLON);
        for (Channel channel : RaftNode.IP_CHANNELS_WITH_CLIENT_MAP.get(ipAndCommandId[0])) {
            channel.writeAndFlush((ipAndCommandId[1] + Const.COMMA + Const.TRUE).getBytes(UTF_8));
        }
    }


    /**
     * 处理同步任期请求
     */
    private void handleSyncTermReq(long opposingTerm, ChannelHandlerContext channelHandlerContext) {
        if (opposingTerm < RaftNode.term) {
            channelHandlerContext.writeAndFlush((Const.RPC_TOBESLAVE + Const.COMMA + LogService.getTerm()).getBytes(UTF_8));
        } else {
            //失连恢复后,数据(未提交数据)可能不一致
            logMustBeConsistent = false;
            if (Role.FOLLOWER == RaftNode.getRole()) {
                /*
                收到这个请求并且本节点是Follower的情况
                1、挂掉后,重启server
                2、和主节点的网络不通一段时间后回复网络
                3、脑裂后处于少数派,网络分区恢复后(这种情况,对方的任期是比自己要大的)
                4、脑裂后处于多数派,网络分区恢复后(这种情况,对方的任期是比自己要小的)
                */
                if (opposingTerm > RaftNode.term) {
                    RaftNode.updateTerm(RaftNode.term, opposingTerm);
                    LogService.clearUncommittedEntry();
                } else if (opposingTerm == RaftNode.term) {
                    //说明还是同一个主
                    RaftNode.resetTimer();
                }
            } else if (opposingTerm >= RaftNode.term) {
                //1、本节点是网络分区后少数派的leader,脑裂恢复后 2、网络闪断恢复后候选者发现已经有新主了(几乎不可能)
                RaftNode.downgradeToSlaveNode(true, opposingTerm);
            }
        }
    }
}
