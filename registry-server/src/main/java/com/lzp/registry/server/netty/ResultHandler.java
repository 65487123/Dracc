
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
import com.lzp.registry.server.raft.RaftNode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;


/**
 * Description:处理rpc结果的handler
 *
 * @author: Zeping Lu
 * @date: 2021/3/24 19:48
 */
public class ResultHandler extends SimpleChannelInboundHandler<byte[]> {

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, byte[] bytes) {
        String[] commandIdAndResult = new String(bytes).split(Cons.COLON);

        if (Cons.YES.equals(commandIdAndResult[1])) {
            RaftNode.cidAndResultMap.get(commandIdAndResult[0]).countDown();
        } else if (Cons.RPC_TOBESLAVE.equals(commandIdAndResult[0])) {
            //选举时,远端节点任期比本端节点新,会发这个消息
            RaftNode.downgradeToSlaveNode(Long.parseLong(commandIdAndResult[1]));
        } else if (Cons.COPY_LOG_REQ.equals(commandIdAndResult[0])) {
            //放到server的从reactor中执行,以满足单线程模型
            NettyServer.workerGroup.execute(() -> sendOwnState(channelHandlerContext));
        }
    }

    private void sendOwnState(ChannelHandlerContext channelHandlerContext) {
        channelHandlerContext.writeAndFlush()
    }
}
