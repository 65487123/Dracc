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

 import com.lzp.dracc.server.raft.RaftNode;
 import com.lzp.dracc.server.raft.Role;
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
                 if (Role.FOLLOWER == RaftNode.getRole()) {
                     RaftNode.resetTimer();
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
