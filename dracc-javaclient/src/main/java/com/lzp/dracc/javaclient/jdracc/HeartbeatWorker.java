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

import com.lzp.dracc.common.util.ThreadFactoryImpl;
import io.netty.channel.Channel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HeartbeatWorker {
    
    private static final ThreadPoolExecutor SINGLE_THREAD_POOL = new ThreadPoolExecutor(1,
            1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("hearbeat"));
    private static final List<Channel> CHANNEL_LIST = new CopyOnWriteArrayList<>();
    
    static {
        SINGLE_THREAD_POOL.execute(() -> {
            byte[] emptyPackage = new byte[0];
            for (; ; ) {
                for (Channel channel : CHANNEL_LIST) {
                    channel.writeAndFlush(emptyPackage);
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {
                }
            }
        });
    }


    public static void executeHeartBeat(Channel channel) {
        CHANNEL_LIST.add(channel);
    }

    public static void stopHeartBeat(Channel channel) {
        CHANNEL_LIST.remove(channel);
    }

}
