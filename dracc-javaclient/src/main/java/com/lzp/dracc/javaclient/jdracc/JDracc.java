
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
import com.lzp.dracc.javaclient.EventListener;
import com.lzp.dracc.javaclient.api.DraccClient;
import com.lzp.dracc.javaclient.exception.DraccException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * Description:Dracc java客户端实现
 *
 * @author: Zeping Lu
 * @date: 2021/3/24 19:48
 */
public class JDracc implements DraccClient, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDracc.class);


    private Channel channelToLeader;
    private int timeout;

    /**
     * 创建一个Dracc集群的客户端
     *
     * @param timeout     读写dracc的超时时间,单位是毫秒,超过这个时间没响应会抛出异常
     * @param ipAndPorts dracc集群所有节点的ip及端口
     * @throws DraccException   exception
     */
    public JDracc(int timeout, String... ipAndPorts) throws InterruptedException, DraccException {
        setUpChannelToLeader(ipAndPorts);
        this.timeout = timeout;
    }


    private void setUpChannelToLeader(String... ipAndPorts) throws InterruptedException, DraccException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ExecutorService threadPool = new ThreadPoolExecutor(ipAndPorts.length, ipAndPorts.length, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryImpl("find leader"));
        for (String ipAndPort : ipAndPorts) {
            threadPool.execute(() -> findLeaderAndSetChannel(ipAndPort, countDownLatch));
        }
        boolean leaderFound = false;
        try {
            if (!(leaderFound = countDownLatch.await(5, TimeUnit.SECONDS))) {
                throw new DraccException("can not find leader");
            }
        } finally {
            threadPool.shutdownNow();
            if (leaderFound) {
                //事件监听放到这是为了防止当server端有两个主时添加监听的连接被第二个建立的连接覆盖了
                channelToLeader.closeFuture().addListener((ChannelFutureListener) future -> {
                    resetChannelToLeader(ipAndPorts);
                });
            }
        }
    }


    private void resetChannelToLeader(String... ipAndPorts) {
        try {
            Thread.sleep(1000);
            LOGGER.warn("Lost connection with the leader or reset ChannelToleader failed, set ChannelToleader again");
            setUpChannelToLeader(ipAndPorts);
        } catch (Exception e) {
            resetChannelToLeader(ipAndPorts);
        }
    }


    private void findLeaderAndSetChannel(String ipAndPort, CountDownLatch countDownLatch) {
        Channel channel = ConnectionFactory.newChannel(ipAndPort);
        Thread thisThread = Thread.currentThread();
        ResultHandler.ThreadResultAndTime threadResultAndTime = new ResultHandler.ThreadResultAndTime(System.currentTimeMillis() + 5000, thisThread);
        ResultHandler.reqIdThreadMap.put(thisThread.getId(), threadResultAndTime);
        channel.writeAndFlush(Const.RPC_GETROLE.getBytes(StandardCharsets.UTF_8));
        Object result;
        while ((result = threadResultAndTime.getResult()) == null) {
            LockSupport.park(thisThread);
        }
        if ("LEADER".equals(result) && countDownLatch.getCount() != 0) {
            channelToLeader = channel;
            countDownLatch.countDown();
        } else {
            channel.close();
        }
    }


    @Override
    public void registerInstance(String serviceName, String ip, int port) throws DraccException {

    }

    @Override
    public void deregisterInstance(String serviceName, String ip, int port) throws DraccException {

    }

    @Override
    public List<String> getAllInstances(String serviceName) throws DraccException {
        return null;
    }

    @Override
    public void subscribe(String serviceName, EventListener listener) throws DraccException {

    }

    @Override
    public void unsubscribe(String serviceName, EventListener listener) throws DraccException {

    }

    @Override
    public void addConfig(String configName, String configVal) throws DraccException {

    }

    @Override
    public void removeConfig(String configName, String configVal) throws DraccException {

    }

    @Override
    public List<String> getConfigs(String configName) throws DraccException {
        return null;
    }

    @Override
    public void acquireDistributedLock(String lockName) throws DraccException {

    }

    @Override
    public void releaseDistributedlock(String lockName) throws DraccException {

    }


    @Override
    public void close() throws Exception {

    }
}
