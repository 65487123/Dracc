
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

import com.lzp.dracc.common.constant.Const;
import com.lzp.dracc.common.util.StringUtil;
import com.lzp.dracc.server.raft.LogService;
import com.lzp.dracc.server.raft.RaftNode;
import com.lzp.dracc.server.util.Data;
import com.lzp.dracc.server.util.DataSearialUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Description:处理rpc结果的handler
 *
 * @author: Zeping Lu
 * @date: 2021/3/24 19:48
 */
public class ResultHandler extends SimpleChannelInboundHandler<byte[]> {

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, byte[] bytes) {
        String[] message = StringUtil.stringSplit(new String(bytes, UTF_8), Const.COMMA);

        if (Const.YES.equals(message[1])) {
            RaftNode.cidAndResultMap.get(message[0]).countDown();
        } else if (Const.RPC_TOBESLAVE.equals(message[0])) {
            //选举时,远端节点任期比本端节点新,或者当届任期已经有主,会发这个消息
            RaftNode.downgradeToSlaveNode(false, Long.parseLong(message[1]));
        } else if (Const.COPY_LOG_REQ.equals(message[0])) {
            //放到server的从reactor中执行,以满足单线程模型
            NettyServer.workerGroup.execute(() -> sendOwnState(Long.parseLong(message[1]), channelHandlerContext));
        }
    }


    /**
     * Description:
     * 把本节点状态(状态机、日志等)传到对端
     */
    private void sendOwnState(long remoteCommittedIndex, ChannelHandlerContext channelHandlerContext) {
        LogService.waitUntilAllLogWriteComplete();
        if (LogService.getCommittedLogIndex() == remoteCommittedIndex) {
            // 增量同步：只同步未提交日志（Base64 编码避免分隔符冲突）
            String uncommittedBase64 = Base64.getEncoder().encodeToString(LogService.getFileContentOfUncommittedEntry());
            String msg = "x" + Const.COMMAND_SEPARATOR + "1" + Const.COMMAND_SEPARATOR + uncommittedBase64;
            channelHandlerContext.writeAndFlush(msg.getBytes(UTF_8));
        } else {
            // 全量同步：已提交日志、未提交日志、状态机快照、coveredIndex 均需安全传输

            String committedBase64 = Base64.getEncoder().encodeToString(LogService.getFileContentOfCommittedEntry());
            String uncommittedBase64 = Base64.getEncoder().encodeToString(LogService.getFileContentOfUncommittedEntry());
            String snapshotBase64 = Base64.getEncoder().encodeToString(LogService.getFileContentOfSnapshot());

            String msg = "x" + Const.COMMAND_SEPARATOR + "0" + Const.COMMAND_SEPARATOR
                    + committedBase64 + Const.COMMAND_SEPARATOR
                    + uncommittedBase64 + Const.COMMAND_SEPARATOR
                    + snapshotBase64 + Const.COMMAND_SEPARATOR
                    + LogService.getCoveredIndex();

            channelHandlerContext.writeAndFlush(msg.getBytes(UTF_8));
        }
    }
}
