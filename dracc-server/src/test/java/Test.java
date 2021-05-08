
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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.net.Socket;

/**
 * Description:
 *
 * @author: Zeping Lu
 * @date: 2021/5/8 18:03
 */
public class Test {
    public static void main(String[] args) throws InterruptedException, IOException {
        /*new Bootstrap().group(new NioEventLoopGroup(1)).channel(NioSocketChannel.class).handler(new ChannelInitializer() {

            @Override
            protected void initChannel(Channel ch) throws Exception {

            }
        }).connect("127.0.0.1",6666).sync();*/
        Socket socket = new Socket("10.240.70.180",22);
        socket.close();
    }
}
