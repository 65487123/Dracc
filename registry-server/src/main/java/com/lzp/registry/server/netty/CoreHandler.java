
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

import com.lzp.registry.common.constant.Cons;
import com.lzp.registry.common.util.CommonUtil;
import com.lzp.registry.common.util.ThreadFactoryImpl;
import com.lzp.registry.server.raft.LogService;
import com.lzp.registry.server.raft.RaftNode;
import com.lzp.registry.server.util.CountDownLatch;
import com.lzp.registry.server.util.ThreadPoolExecutor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;


/**
 * Description:处理rpc请求具体内容的handler
 *
 * @author: Zeping Lu
 * @date: 2021/3/19 10:56
 */
public class CoreHandler extends SimpleChannelInboundHandler<byte[]> {

    private static ExecutorService REPLICATION_THREAD_POOL;
    public static List<Channel> slaves;

    {
        REPLICATION_THREAD_POOL = new ThreadPoolExecutor(1, 1, 0, new ArrayBlockingQueue<Runnable>(500),
                new ThreadFactoryImpl("replication thread"), (r, executor) -> r.run());
    }

    public static void resetReplicationThreadPool(){
        REPLICATION_THREAD_POOL.shutdownNow();
        REPLICATION_THREAD_POOL = new ThreadPoolExecutor(1, 1, 0, new ArrayBlockingQueue<Runnable>(500),
                new ThreadFactoryImpl("replication thread"), (r, executor) -> r.run());
    }

    /**
     * 把命令根据分隔符分割获得一个字符串数组
     * 字符串数组第二个位置就是具体的请求命令
     * 1、如果这个节点是从节点，数组第一个位置就是这个请求的请求id
     * 2、如果这个节点是主节点，数组第一个位置是读写请求的具体请求类型(add、get等),第三位置是key
     */
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, byte[] bytes) {
        String[] command = new String(bytes).split(Cons.COMMAND_SEPARATOR);
        switch (command[1]) {
            case Cons.RPC_ASKFORVOTE: {
                voteIfAppropriate(channelHandlerContext, command);
            }
            case Cons.RPC_FROMCLIENT: {
                handleClientReq(command, channelHandlerContext);
            }
            case Cons.RPC_REPLICATION: {

            }
            default: {

            }
        }
    }

    /**
     * 收到投票请求,如果合适就投对方一票
     */
    private void voteIfAppropriate(ChannelHandlerContext channelHandlerContext, String[] command) {
        if (Cons.LEADER.equals(RaftNode.getRole())) {
            channelHandlerContext.writeAndFlush((Cons.RPC_TOBESLAVE + Cons.COLON + LogService.getTerm()).getBytes());
        } else if (Cons.FOLLOWER.equals(RaftNode.getRole())) {
            long opposingTerm = Long.parseLong(command[2]);
            if (opposingTerm > RaftNode.term &&
                    Long.parseLong(command[3]) >= LogService.getLogIndex()) {
                RaftNode.increaseTerm();
                RaftNode.ResetTimer();
                channelHandlerContext.writeAndFlush((command[0] + Cons.COLON + Cons.YES).getBytes());
            } else if (opposingTerm < RaftNode.term) {
                channelHandlerContext.writeAndFlush((Cons.RPC_TOBESLAVE + Cons.COLON + LogService.getTerm()).getBytes());
            }
        }
    }

    /**
     * 判断从客户端发来的写请求是否会改变状态机(写操作是否能写成功)
     * 如果不会,则不必向从节点发送请求,直接返回结果
     */
    private boolean checkWillChangeTheStateMachine(String[] command) {
        Map<String, Set<String>> data = RaftNode.DATA;
        switch (command[0]) {
            case Cons.ADD: {
                return !data.get(command[2]).contains(command[3]);
            }
            case Cons.REM: {
                return data.get(command[2]).contains(command[3]);
            }
            default: {
                return false;
            }
        }
    }

    /**
     * 处理写请求
     */
    private void handleClientReq(String[] command, ChannelHandlerContext channelHandlerContext) {
        if (Cons.GET.equals(command[0])) {
            if (RaftNode.termAndSlaveChannels.get(String.valueOf(RaftNode.term)).size() < RaftNode.HALF_COUNT) {
                channelHandlerContext.writeAndFlush((Cons.EXCEPTION + Cons.READ_FAIL_MESSAGE).getBytes());
            } else {
                channelHandlerContext.writeAndFlush(CommonUtil.serial(RaftNode.DATA.get(command[2])).getBytes());
            }
        } else {
            if (checkWillChangeTheStateMachine(command)) {
                CountDownLatch countDownLatch = new CountDownLatch(RaftNode.HALF_COUNT);
                String commandId;
                RaftNode.cidAndResultMap.put(commandId = RaftNode.getCommandId(), countDownLatch);
                for (Channel channel : slaves) {
                    channel.writeAndFlush(commandId + Cons.COMMAND_SEPARATOR + Cons.RPC_REPLICATION + command[0] + Cons
                            .RPC_REPLICATION + command[2] + Cons.RPC_REPLICATION + command[3]);
                }
                REPLICATION_THREAD_POOL.execute(new Runnable() {
                    @Override
                    public void run() {

                    }
                });
                countDownLatch.await();
                channelHandlerContext.writeAndFlush()
            } else {
                channelHandlerContext.writeAndFlush(Cons.FALSE.getBytes());
            }
        }
    }

    private

}
