
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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
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

    /*static {
        bootstrap.group(workerGroup).channel(NioSocketChannel.class).handler(new ChannelInitializer() {
            @Override
            protected void initChannel(Channel channel) {
                channel.pipeline().addLast(new IdleStateHandler(15, Integer.MAX_VALUE, Integer.MAX_VALUE))
                        .addLast(new MessageDecoder()).addLast(new LzpMessageEncoder())
                        .addLast("resultHandler", new ResultHandler());
            }
        });
    }*/








    @Override
    public void close() {
        workerGroup.shutdownGracefully();
    }



}
