
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


import com.lzp.dracc.common.constant.Const;
import com.lzp.dracc.common.util.ThreadFactoryImpl;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * Description:处理rpc结果的handler
 *
 * @author: Zeping Lu
 * @date: 2021/3/24 19:48
 */
public class ResultHandler extends SimpleChannelInboundHandler<byte[]> {


    /**
     * Description:用来存超时时刻和线程以及rpc结果
     */
    public static class ThreadResultAndTime {
        /**
         * 过期的具体时刻
         */
        private long deadLine;
        /**
         * 被阻塞的线程
         */
        private Thread thread;
        /**
         * rpc结果
         */
        private volatile Object result;

        public ThreadResultAndTime(long deadLine, Thread thread) {
            this.deadLine = deadLine;
            this.thread = thread;
        }

        public Object getResult() {
            return result;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultHandler.class);

    /**
     * Description:线程池
     */
    private static ExecutorService rpcClientThreadPool;
    /**
     * Description:key是发起rpc请求后被阻塞的线程id，value是待唤醒的线程和超时时间
     */
    public static Map<Long, ThreadResultAndTime> reqIdThreadMap = new ConcurrentHashMap<>();


    static {
        rpcClientThreadPool = new ThreadPoolExecutor(3, 3, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000),
                new ThreadFactoryImpl("rpc client"), (r, executor) -> r.run());



        //一个线程专门用来检测rpc超时
        rpcClientThreadPool.execute(() -> {
            long now;
            while (true) {
                now = System.currentTimeMillis();
                for (Map.Entry<Long, ThreadResultAndTime> entry : reqIdThreadMap.entrySet()) {
                    //漏网之鱼会在下次被揪出来
                    if (entry.getValue().deadLine < now) {
                        ThreadResultAndTime threadResultAndTime = reqIdThreadMap.remove(entry.getKey());
                        threadResultAndTime.result = Const.EXCEPTION + Const.TIMEOUT;
                        LockSupport.unpark(threadResultAndTime.thread);
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage(),e);
                }
            }
        });
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, byte[] bytes) {
        String[] threadIdAndResult = new String(bytes, StandardCharsets.UTF_8).split(Const.COLON);
        ThreadResultAndTime threadResultAndTime = reqIdThreadMap.remove(Long.parseLong(threadIdAndResult[0]));
        if (threadResultAndTime != null) {
            threadResultAndTime.result = new String(bytes, StandardCharsets.UTF_8);
            LockSupport.unpark(threadResultAndTime.thread);
        }
    }


}
