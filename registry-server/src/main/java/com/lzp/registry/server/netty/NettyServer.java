
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

import com.lzp.registry.common.zpproto.LzpMessageEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description:nettyServer
 *
 * @author: Zeping Lu
 * @date: 2021/3/18 10:48
 */
public class NettyServer {
    public static EventLoopGroup workerGroup = new NioEventLoopGroup(1);
    private static EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final static Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

    /**
     * 启动nettyserver。不考虑意外释放端口情况(几乎是挂了)
     */
    public static void start(String ip, int port) {
        try {
            new ServerBootstrap().group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer() {
                @Override
                protected void initChannel(Channel channel) { channel.pipeline()
                        .addLast(new IdleStateHandler(15, Integer.MAX_VALUE, Integer.MAX_VALUE))
                        .addLast(new LzpRaftMessageDecoder(true)).addLast(new LzpMessageEncoder())
                        .addLast("serviceHandler", new CoreHandler());
                }
            }).bind(ip, port).sync();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
