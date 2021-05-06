
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

package com.lzp.dracc.javaclient.jdracc;


/**
 * Description:处理rpc结果的handler
 *
 * @author: Zeping Lu
 * @date: 2021/3/24 19:48
 */
/*public class ResultHandler extends SimpleChannelInboundHandler<byte[]> {


    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, byte[] bytes) {
        String[] message = new String(bytes, UTF_8).split(Const.COLON);

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


    *//**
     * Description:
     * 把本节点状态(状态机、日志等)传到对端
     *//*
    private void sendOwnState(long remoteCommittedIndex, ChannelHandlerContext channelHandlerContext) {
        if (LogService.getCommittedLogIndex() == remoteCommittedIndex) {
            //说明状态机一样,只需要同步未提交日志就行
            channelHandlerContext.writeAndFlush(("x" + Const.COMMAND_SEPARATOR + "1" + Const
                    .COMMAND_SEPARATOR + LogService.getFileContentOfUncommittedEntry()).getBytes(UTF_8));
        } else {
            //需要全量同步
            channelHandlerContext.writeAndFlush(("x" + Const.COMMAND_SEPARATOR + "1" + Const
                    .COMMAND_SEPARATOR + LogService.getFileContentOfCommittedEntry() + Const
                    .COMMAND_SEPARATOR + LogService.getFileContentOfUncommittedEntry() + Const
                    .COMMAND_SEPARATOR + new String(DataSearialUtil.serialize(new Data(RaftNode
                    .data)), UTF_8) + Const.COMMAND_SEPARATOR + LogService.getCoveredIndex())
                    .getBytes(UTF_8));
        }
    }
}*/
