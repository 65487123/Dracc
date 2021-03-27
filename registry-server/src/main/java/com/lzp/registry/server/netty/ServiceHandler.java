
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
import com.lzp.registry.server.raft.LogService;
import com.lzp.registry.server.raft.RaftNode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;


/**
 * Description:处理业务逻辑的handler
 *
 * @author: Zeping Lu
 * @date: 2021/3/19 10:56
 */
public class ServiceHandler extends SimpleChannelInboundHandler<byte[]> {


    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, byte[] bytes) {
        String[] command = new String(bytes).split(Cons.COMMAND_SEPARATOR);
        switch (command[1]) {
            case Cons.RPC_ASKFORVOTE: {
                voteIfAppropriate(channelHandlerContext, command);
            }
            //TODO 真正的业务请求
            case "": {

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
            if (opposingTerm > RaftNode.getTerm() &&
                    Long.parseLong(command[3]) >= LogService.getLogIndex()) {
                RaftNode.increaseTerm();
                RaftNode.ResetTimer();
                channelHandlerContext.writeAndFlush((command[0] + Cons.COLON + Cons.YES).getBytes());
            } else if (opposingTerm < RaftNode.getTerm()) {
                channelHandlerContext.writeAndFlush((Cons.RPC_TOBESLAVE + Cons.COLON + LogService.getTerm()).getBytes());
            }
        }
    }
}
