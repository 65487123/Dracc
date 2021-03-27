
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
        if (Cons.RPC_TOBESLAVE.equals(commandIdAndResult[0])) {
            /* 收到这个消息的情况
             * 1、选举时,远端节点任期比本端节点新
             * 2、发起选举时候，收到新主的心跳(不论是否网络分区恢复)
             * 3、脑裂恢复后,这个主节点向另一个从节点发送心跳,对方会回这个消息,
             * 任期低的会降为从节点(因为任期高的是后选举出来的,后选举出来说明是多数派分区)
             */
            long opposingTerm = Long.parseLong(commandIdAndResult[1]);
            if (opposingTerm >= RaftNode.getTerm()) {
                long preTerm = RaftNode.updateTerm(opposingTerm);
                RaftNode.downgradeToSlaveNode(preTerm);
            }
        } else if (Cons.YES.equals(commandIdAndResult[1])) {
            RaftNode.getCidAndResultMap().get(commandIdAndResult[0]).countDown();
        }

    }
}
