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
 import io.netty.buffer.ByteBuf;
 import io.netty.channel.ChannelHandlerContext;
 import io.netty.handler.codec.ReplayingDecoder;
 import io.netty.handler.timeout.IdleStateEvent;

 import java.util.List;

 /**
  * @author zeping lu
  */
 public class LzpRaftMessageDecoder extends ReplayingDecoder<Void> {
     private final boolean isServer;

     public LzpRaftMessageDecoder(boolean isServer) {
         this.isServer = isServer;
     }

     @Override
     protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) {
         int length = byteBuf.readInt();
         if (length == 0) {
             if (isServer) {
                 channelHandlerContext.channel().writeAndFlush(new byte[0]);
                 if (Cons.FOLLOWER.equals(RaftNode.getRole())) {
                     RaftNode.ResetTimer();
                 } else {
                     //脑裂恢复的情况或者候选者发现已经有新主了
                     channelHandlerContext.channel().writeAndFlush((Cons.RPC_TOBESLAVE + Cons.COLON + LogService.getTerm()).getBytes());
                 }
             }
             return;
         }
         byte[] content = new byte[length];
         byteBuf.readBytes(content);
         list.add(content);
     }

     @Override
     public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
         if (evt instanceof IdleStateEvent) {
             ctx.channel().close();
         }
     }

 }
