
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

import com.lzp.dracc.common.constant.Command;
import com.lzp.dracc.common.constant.Const;
import com.lzp.dracc.common.util.CommonUtil;
import com.lzp.dracc.common.util.ThreadFactoryImpl;
import com.lzp.dracc.javaclient.EventListener;
import com.lzp.dracc.javaclient.api.DraccClient;
import com.lzp.dracc.javaclient.exception.DraccException;
import com.lzp.dracc.javaclient.exception.TheClusterIsDownException;
import com.lzp.dracc.javaclient.exception.TimeoutException;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * Description:Dracc java客户端实现
 *
 * 注意:用来注册和注销服务实例的客户端实须一直保证存活(不要调用close(),并且保持对这个对象的引用),不然可能会出问题。
 * 建议整个JVM都用一个JDracc实例
 *
 * @author: Zeping Lu
 * @date: 2021/3/24 19:48
 */
public class JDracc implements DraccClient, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDracc.class);


    private Channel channelToLeader;
    private final int TIMEOUT;

    /**
     * 创建一个Dracc集群的客户端
     *
     * @param timeout     读写dracc的超时时间,单位是毫秒,超过这个时间没响应会抛出异常
     * @param ipAndPorts dracc集群所有节点的ip及端口
     * @throws DraccException   exception
     */
    public JDracc(int timeout, String... ipAndPorts) throws InterruptedException, DraccException {
        setUpChannelToLeader(ipAndPorts);
        this.TIMEOUT = timeout;
    }


    private void setUpChannelToLeader(String... ipAndPorts) throws InterruptedException, DraccException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ExecutorService threadPool = new ThreadPoolExecutor(ipAndPorts.length, ipAndPorts.length, 0,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryImpl("find leader"));
        for (String ipAndPort : ipAndPorts) {
            threadPool.execute(() -> findLeaderAndSetChannel(ipAndPort, countDownLatch));
        }
        try {
            if (countDownLatch.await(5, TimeUnit.SECONDS)) {
                synchronized (JDracc.class) {
                    HeatbeatWorker.executeHeartBeat(channelToLeader.closeFuture().addListener(future -> {
                        HeatbeatWorker.stopHeartBeat(channelToLeader);
                        resetChannelToLeader(ipAndPorts);
                        //TODO 重新执行一边注册实例操作
                    }).channel());
                }
            } else {
                throw new DraccException("can not find leader");
            }
        } finally {
            threadPool.shutdownNow();
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
        String result;
        while ((result = threadResultAndTime.getResult()) == null) {
            LockSupport.park();
        }
        if ("LEADER".equals(result)) {
            synchronized (JDracc.class) {
                if (countDownLatch.getCount() != 0) {
                    channelToLeader = channel;
                    countDownLatch.countDown();
                }
            }
        } else {
            channel.close();
        }
    }

    private String sentRpcAndGetResult(Thread currentThread,String command){
        ResultHandler.ThreadResultAndTime threadResultAndTime = new ResultHandler.ThreadResultAndTime(System.currentTimeMillis() + TIMEOUT, currentThread);
        ResultHandler.reqIdThreadMap.put(currentThread.getId(), threadResultAndTime);
        channelToLeader.writeAndFlush(command.getBytes(StandardCharsets.UTF_8));
        String result;
        while ((result = threadResultAndTime.getResult()) == null) {
            LockSupport.park();
        }
        return result;
    }


    private String generateCommand(long threadId, String dataType, String operType, String key, String value) {
        return threadId + Const.RPC_FROMCLIENT + Const.RPC_FROMCLIENT + Const.COMMAND_SEPARATOR
                + dataType + Const.COMMAND_SEPARATOR + operType + Const.COMMAND_SEPARATOR
                + key + Const.COMMAND_SEPARATOR + value;
    }

    private String genCmdForGet(long threadId, String dataType, String operType, String key) {
        return threadId + Const.RPC_FROMCLIENT + Const.RPC_FROMCLIENT + Const.COMMAND_SEPARATOR
                + dataType + Const.COMMAND_SEPARATOR + operType + Const.COMMAND_SEPARATOR + key;
    }

    private void checkResult(String result) throws DraccException {
        if (result.startsWith(Const.EXCEPTION)) {
            String content = result.substring(1);
            if (Const.TIMEOUT.equals(content)) {
                throw new TimeoutException();
            } else if (Const.CLUSTER_DOWN_MESSAGE.equals(content)) {
                throw new TheClusterIsDownException();
            } else {
                throw new DraccException(content);
            }
        }
    }

    @Override
    public void registerInstance(String serviceName, String ip, int port) throws DraccException {
        Thread currentThread = Thread.currentThread();
        checkResult(sentRpcAndGetResult(currentThread, generateCommand(currentThread
                .getId(), Const.ZERO, Command.ADD, serviceName, ip + Const.COLON + port)));

    }

    @Override
    public void deregisterInstance(String serviceName, String ip, int port) throws DraccException {
        Thread currentThread = Thread.currentThread();
        checkResult(sentRpcAndGetResult(currentThread, generateCommand(currentThread
                .getId(), Const.ZERO, Command.REM, serviceName, ip + Const.COLON + port)));
    }

    @Override
    public List<String> getAllInstances(String serviceName) throws DraccException {
        Thread currentThread = Thread.currentThread();
        String result = sentRpcAndGetResult(currentThread, genCmdForGet(currentThread
                .getId(), Const.ZERO, Command.GET, serviceName));
        try {
            return CommonUtil.deserial(result);
        } catch (Exception e) {
            checkResult(result);
            return null;
        }
    }

    @Override
    public void subscribe(String serviceName, EventListener listener) throws DraccException {
        Thread currentThread = Thread.currentThread();
        checkResult(sentRpcAndGetResult(currentThread, generateCommand(currentThread
                .getId(), Const.ONE, Command.ADD, serviceName,
                channelToLeader.localAddress() + Const.COLON + channelToLeader.localAddress())));
    }

    @Override
    public void unsubscribe(String serviceName, EventListener listener) throws DraccException {
        Thread currentThread = Thread.currentThread();
        checkResult(sentRpcAndGetResult(currentThread, generateCommand(currentThread
                .getId(), Const.ONE, Command.REM, serviceName,
                channelToLeader.localAddress() + Const.COLON + channelToLeader.localAddress())));
    }

    @Override
    public void addConfig(String configName, String configVal) throws DraccException {
        Thread currentThread = Thread.currentThread();
        checkResult(sentRpcAndGetResult(currentThread, generateCommand(currentThread
                .getId(), Const.ONE, Command.ADD, configName, configVal)));
    }

    @Override
    public void removeConfig(String configName, String configVal) throws DraccException {
        Thread currentThread = Thread.currentThread();
        checkResult(sentRpcAndGetResult(currentThread, generateCommand(currentThread
                .getId(), Const.ONE, Command.REM, configName, configVal)));
    }

    @Override
    public List<String> getConfigs(String configName) throws DraccException {
        Thread currentThread = Thread.currentThread();
        String result = sentRpcAndGetResult(currentThread, genCmdForGet(currentThread
                .getId(), Const.ONE, Command.GET, configName));
        try {
            return CommonUtil.deserial(result);
        } catch (Exception e) {
            checkResult(result);
            return null;
        }
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
