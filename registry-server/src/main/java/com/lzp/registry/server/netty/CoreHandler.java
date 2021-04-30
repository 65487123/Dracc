
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

package com.lzp.registry.server.netty;

import com.lzp.registry.common.constant.Command;
import com.lzp.registry.common.constant.Cons;
import com.lzp.registry.common.constant.Role;
import com.lzp.registry.common.util.CommonUtil;
import com.lzp.registry.common.util.ThreadFactoryImpl;
import com.lzp.registry.server.raft.LogService;
import com.lzp.registry.server.raft.RaftNode;
import com.lzp.registry.server.util.CountDownLatch;
import com.lzp.registry.server.util.ThreadPoolExecutor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Description:处理rpc请求核心handler
 *
 * @author: Zeping Lu
 * @date: 2021/3/19 10:56
 */
public class CoreHandler extends SimpleChannelInboundHandler<byte[]> {

    private final static Logger LOGGER = LoggerFactory.getLogger(CoreHandler.class);
    private static ExecutorService repilicationThreadPool;
    public static List<Channel> slaves;
    private final ExecutorService SINGLE_THREAD_POOL = new ThreadPoolExecutor(1, 1, 0
            , new LinkedBlockingQueue(), new ThreadFactoryImpl("replication thread when log not synced"));
    /**
     * 单线程操作(一个io线程),无需加volatile
     */
    private boolean logMustBeConsistent = false;

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        SINGLE_THREAD_POOL.shutdownNow();
    }

    {
        repilicationThreadPool = new ThreadPoolExecutor(1, 1, 0, new ArrayBlockingQueue<Runnable>(1000),
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
     * 字符串数组第二个位置就是具体的请求命令
     * 1、如果这个节点是从节点，数组第一个位置就是这个请求的请求id
     * 2、如果这个节点是主节点，数组第一个位置是读写请求的具体请求类型(add、get等),第三位置是key
     */
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, byte[] bytes) {
        String[] command = new String(bytes, UTF_8).split(Cons.COMMAND_SEPARATOR);
        //分支不是特别多的情况下,if/else性能比switch要高,尤其是把高频率的分支放在前面
        if (Cons.RPC_FROMCLIENT.equals(command[1])) {
            handleClientReq(command, channelHandlerContext);
        } else if (Cons.RPC_REPLICATION.equals(command[1])) {
            handleReplicationReq(command, channelHandlerContext);
        } else if (Cons.RPC_COMMIT.equals(command[1])) {
            LogService.commitFirstUncommittedLog();
        } else if (Cons.RPC_SYNC_TERM.equals(command[1])) {
            handleSyncTermReq(Long.parseLong(command[2]));
        } else if (Cons.RPC_ASKFORVOTE.equals(command[1])) {
            voteIfAppropriate(channelHandlerContext, command);
        } else {
            //Cons.COPY_LOG_REPLY.equals(command[1])
            syncLogAndStateMachine(command);
        }
    }

    /**
     * 同步日志以及状态机
     */
    private synchronized void syncLogAndStateMachine(String[] command) {
        //0表示是全量同步
        if ("0".equals(command[1])) {
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
        * */
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
        channelHandlerContext.writeAndFlush((command[0] + Cons.COLON + Cons.YES).getBytes(UTF_8));
    }

    /**
     * 等待同步日志成功,然后回日志复制成功结果
     */
    private synchronized void waitUntilSyncThenReturnYes(String[] command, ChannelHandlerContext channelHandlerContext) {
        if (!logMustBeConsistent) {
            channelHandlerContext.writeAndFlush((Cons.COPY_LOG_REQ + Cons.COLON + LogService
                    .getCommittedLogIndex()).getBytes(UTF_8));
            while (!logMustBeConsistent) {
                try {
                    this.wait();
                } catch (InterruptedException ignored) {
                }
            }
        }
        channelHandlerContext.writeAndFlush((command[0] + Cons.COLON + Cons.YES).getBytes(UTF_8));
    }

    /**
     * 收到投票请求,如果合适就投对方一票
     */
    private void voteIfAppropriate(ChannelHandlerContext channelHandlerContext, String[] command) {
        long opposingTerm = Long.parseLong(command[2]);
        if (Role.FOLLOWER.equals(RaftNode.getRole())) {
            if (opposingTerm > RaftNode.term && Long.parseLong(command[3]) >= LogService.getCommittedLogIndex()
                    && Long.parseLong(command[4]) >= LogService.getUncommittedLogSize()) {
                RaftNode.updateTerm(RaftNode.term, opposingTerm);
                RaftNode.resetTimer();
                channelHandlerContext.writeAndFlush((command[0] + Cons.COLON + Cons.YES).getBytes(UTF_8));
            } else if (opposingTerm < RaftNode.term) {
                channelHandlerContext.writeAndFlush((Cons.RPC_TOBESLAVE + Cons.COLON + LogService.getTerm()).getBytes(UTF_8));
            }
        } else if (Role.LEADER.equals(RaftNode.getRole())) {
            if (opposingTerm > RaftNode.term) {
                //1、网络分区后,对方处于少数派 2、对方和本节点同时发起选举,但是对方竞选失败发起下一轮选举
                //如果对方能成功竞选,少这一票也能竞选成功,所以这里就先不去判断日志然后投票了
                RaftNode.downgradeToSlaveNode(false, opposingTerm);
            } else {
                //比对方先取得半数票,已经竞选成功
                channelHandlerContext.writeAndFlush((Cons.RPC_TOBESLAVE + Cons.COLON + LogService.getTerm()).getBytes(UTF_8));
            }
        }
    }

    /**
     * 判断从客户端发来的写请求是否会改变状态机(写操作是否能写成功)
     * 如果不会,则不必向从节点发送请求,直接返回结果
     */
    private boolean checkWillChangeTheStateMachine(String[] command) {
        Map<String, Set<String>> data = RaftNode.data;
        if (Command.ADD.equals(command[0])) {
            return !data.get(command[2]).contains(command[3]);
        } else if (Command.REM.equals(command[0])) {
            return data.get(command[2]).contains(command[3]);
        }
        return false;
    }

    /**
     * 处理客户端具体请求
     */
    private void handleClientReq(String[] command, ChannelHandlerContext channelHandlerContext) {
        //当少于半数节点存活,整个集群是不可用的,直接返回异常
        if (RaftNode.termAndSlaveChannels.get(String.valueOf(RaftNode.term)).size() < RaftNode.HALF_COUNT) {
            channelHandlerContext.writeAndFlush((Cons.EXCEPTION + Cons.CLUSTER_DOWN_MESSAGE).getBytes(UTF_8));
        } else {
            if (Command.GET.equals(command[0])) {
                channelHandlerContext.writeAndFlush(CommonUtil.serial(RaftNode.data.get(command[2])).getBytes(UTF_8));
            } else {
                handleWriteReq(command, channelHandlerContext);
            }
        }
    }

    /**
     * 处理写请求
     */
    private void handleWriteReq(String[] command, ChannelHandlerContext channelHandlerContext) {
        if (checkWillChangeTheStateMachine(command)) {
            CountDownLatch countDownLatch = new CountDownLatch(RaftNode.HALF_COUNT);
            String commandId;
            RaftNode.cidAndResultMap.put(commandId = RaftNode.getCommandId(), countDownLatch);
            String specificOrder = command[0] + Cons.SPECIFICORDER_SEPARATOR + command[2] + Cons
                    .SPECIFICORDER_SEPARATOR + command[3];
            long unCommittedLogNum = LogService.appendUnCommittedLog(specificOrder);
            for (Channel channel : slaves) {
                channel.writeAndFlush(commandId + Cons.COMMAND_SEPARATOR + Cons.RPC_REPLICATION + Cons
                        .COMMAND_SEPARATOR + specificOrder + Cons.COMMAND_SEPARATOR +
                        LogService.getCommittedLogIndex() + Cons.COMMAND_SEPARATOR + unCommittedLogNum);
            }
            repilicationThreadPool.execute(() -> receiveResponseAndMakeDecision(countDownLatch, command, channelHandlerContext));
        } else {
            channelHandlerContext.writeAndFlush(Cons.FALSE.getBytes(UTF_8));
        }
    }


    /**
     * 等待从节点的响应,并根据具体响应结果做出最终的决定
     */
    private void receiveResponseAndMakeDecision(CountDownLatch countDownLatch, String[] command, ChannelHandlerContext channelHandlerContext) {
        boolean halfAgree = false;
        try {
            halfAgree = countDownLatch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("interrupted when waitting for response from slaves", e);
        }
        if (halfAgree) {
            commitAndReturnResult(command, channelHandlerContext);
        } else {
            //发送日志复制消息前有半数存活,发送日志复制消息时,却有连接断开了,防止数据不一致,重新选主
            RaftNode.downgradeToSlaveNode(false, RaftNode.term);
        }
    }


    /**
     * 提交日志、更新状态机并返回客户端结果
     */
    private void commitAndReturnResult(String[] command, ChannelHandlerContext channelHandlerContext) {
        LogService.commitFirstUncommittedLog();
        Set<String> set;
        if (Command.ADD.equals(command[0])) {
            if ((set = RaftNode.data.get(command[2])) == null) {
                set = new HashSet<>();
            }
            set.add(command[3]);
        } else {
            if ((set = RaftNode.data.get(command[2])) == null) {
                set = new HashSet<>();
            }
            set.remove(command[3]);
        }
        channelHandlerContext.writeAndFlush(Cons.TRUE.getBytes(UTF_8));
        for (Channel channel : slaves) {
            channel.writeAndFlush(("x" + Cons.COMMAND_SEPARATOR + Cons
                    .RPC_COMMIT + Cons.COMMAND_SEPARATOR).getBytes(UTF_8));
        }
    }


    /**
     * 处理同步任期请求
     */
    private void handleSyncTermReq(long opposingTerm) {
        //失连恢复后,数据(未提交数据)可能不一致
        logMustBeConsistent = false;
        if (Role.FOLLOWER.equals(RaftNode.getRole())) {
            /*
            收到这个请求并且是Follower的情况
            1、挂掉后,重启server
            2、和主节点的网络不通一段时间后回复网络
            3、网络分区后处于少数派,网络分区恢复后(这种情况,对方的任期是比自己要大的)
            4、网络分区后处于多数派,网络分区恢复后(这种情况,对方的任期是比自己要小的)
            */
            if (opposingTerm > RaftNode.term) {
                RaftNode.updateTerm(RaftNode.term, opposingTerm);
                LogService.clearUncommittedEntry();
            } else if (opposingTerm == RaftNode.term) {
                //说明还是同一个主
                RaftNode.resetTimer();
            }
        } else {
            //1、脑裂恢复,本节点属于少数派 2、候选者发现已经有新主了(几乎不可能)
            if (opposingTerm >= RaftNode.term) {
                RaftNode.downgradeToSlaveNode(true, opposingTerm);
            }
        }
    }
}
