
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

import com.lzp.dracc.common.util.StringUtil;
import com.lzp.dracc.common.zpproto.LzpMessageEncoder;
import com.lzp.dracc.javaclient.netty.MessageDecoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Description:nettyclient
 *
 * @author: Lu ZePing
 * @date: 2020/9/27 18:32
 */
public class ConnectionFactory implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionFactory.class);
    public static EventLoopGroup workerGroup = new NioEventLoopGroup(1);
    private static Bootstrap bootstrap = new Bootstrap();

    static {
        bootstrap.group(workerGroup).channel(NioSocketChannel.class).handler(new ChannelInitializer() {
            @Override
            protected void initChannel(Channel channel) {
                channel.pipeline().addLast(new IdleStateHandler(12, Integer.MAX_VALUE, Integer.MAX_VALUE))
                        .addLast(new MessageDecoder()).addLast(new LzpMessageEncoder())
                        .addLast("resultHandler", new ResultHandler());
            }
        });
    }

    public static Channel newChannel(String instance) {
        String[] ipAndPort = StringUtil.stringSplit(instance, ':');
        try {
            return bootstrap.connect(ipAndPort[0], Integer.parseInt(ipAndPort[1])).sync().channel();
        } catch (Exception e) {
            LOGGER.warn("set up connection to {} failed , retry", instance);
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
            }
            return newChannel(instance);
        }
    }


    @Override
    public void close() {
        workerGroup.shutdownGracefully();
    }


}
