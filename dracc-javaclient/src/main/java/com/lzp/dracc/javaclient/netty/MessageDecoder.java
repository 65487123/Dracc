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

package com.lzp.dracc.javaclient.netty;


 import io.netty.buffer.ByteBuf;
 import io.netty.channel.ChannelHandlerContext;
 import io.netty.handler.codec.ReplayingDecoder;
 import io.netty.handler.timeout.IdleState;
 import io.netty.handler.timeout.IdleStateEvent;

 import java.util.List;

 /**
  * @author zeping lu
  */
 public class MessageDecoder extends ReplayingDecoder<Void> {


     @Override
     protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) {
         int length = byteBuf.readInt();
         if (length == 0) {
             return;
         }
         byte[] content = new byte[length];
         byteBuf.readBytes(content);
         list.add(content);
     }

     @Override
     public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
         if (evt instanceof IdleStateEvent) {
             IdleStateEvent e = (IdleStateEvent) evt;
             if (e.state() == IdleState.READER_IDLE) {
                 // 读空闲超时，对方可能已经掉线，关闭连接
                 ctx.channel().close();
             } else if (e.state() == IdleState.WRITER_IDLE) {
                 // 写空闲，发送心跳包
                 ctx.channel().writeAndFlush(new byte[0]);
             }
         }
     }

 }
