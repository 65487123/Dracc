
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

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
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

    private volatile boolean isClosed = false;
    private Channel channelToLeader;
    private final int TIMEOUT;

    /**
     * 在本地保存一份通过这个客户端注册的所有实例,当与server失连并且重连后,重新再注册一遍
     * 由于注册实例是幂等操作,server端就算还是存在这个实例,也不会有影响
     */
    private Map<String, Set<String>> registeredInstances = new ConcurrentHashMap<>();



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
                    HeatbeatWorker.executeHeartBeat(channelToLeader.closeFuture()
                            .addListener(future -> onChannelClosed(ipAndPorts)).channel());
                }
            } else {
                throw new DraccException("can not find leader");
            }
        } finally {
            threadPool.shutdownNow();
        }
    }

    private void onChannelClosed(String... ipAndPorts){
        HeatbeatWorker.stopHeartBeat(channelToLeader);
        if (!isClosed) {
            resetChannelToLeader(ipAndPorts);
            try {
                for (Map.Entry<String, Set<String>> entry : registeredInstances.entrySet()) {
                    for (String instance : entry.getValue()) {
                        registerInstance0(entry.getKey(), instance);
                    }
                }
            }catch (Exception ignored){
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
        registerInstance0(serviceName,ip + Const.COLON + port);
    }


    private void registerInstance0(String serviceName, String instance) throws DraccException {
        Thread currentThread = Thread.currentThread();
        checkResult(sentRpcAndGetResult(currentThread, generateCommand(currentThread
                .getId(), Const.ZERO, Command.ADD, serviceName, instance)));
        addInstanceLocally(serviceName, instance);
    }


    @Override
    public void deregisterInstance(String serviceName, String ip, int port) throws DraccException {
        deregisterInstance0(serviceName, ip + Const.COLON + port);
    }

    public void deregisterInstance0(String serviceName, String instance) throws DraccException {
        Thread currentThread = Thread.currentThread();
        checkResult(sentRpcAndGetResult(currentThread, generateCommand(currentThread
                .getId(), Const.ZERO, Command.REM, serviceName, instance)));
        remInstanceLocally(serviceName, instance);
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
        Set<EventListener> eventListeners;
        if ((eventListeners = ResultHandler.serviceNameListenerMap.get(serviceName)) == null) {
            subscribe0(serviceName);
            synchronized (this) {
                if ((eventListeners = ResultHandler.serviceNameListenerMap.get(serviceName)) == null) {
                    eventListeners = new CopyOnWriteArraySet<>();
                }
            }
        } else if (!eventListeners.contains(listener)) {
            subscribe0(serviceName);
        }
        eventListeners.add(listener);
    }


    private void subscribe0(String serviceName) throws DraccException {
        Thread currentThread = Thread.currentThread();
        checkResult(sentRpcAndGetResult(currentThread, generateCommand(currentThread
                        .getId(), Const.ONE, Command.ADD, serviceName,
                ((InetSocketAddress)channelToLeader.localAddress()).getAddress().getHostAddress())));
    }


    @Override
    public void unsubscribe(String serviceName, EventListener listener) throws DraccException {
        Set<EventListener> eventListeners;
        if ((eventListeners = ResultHandler.serviceNameListenerMap.get(serviceName)) != null) {
            eventListeners.remove(listener);
            if (eventListeners.size() == 0) {
                Thread currentThread = Thread.currentThread();
                checkResult(sentRpcAndGetResult(currentThread, generateCommand(currentThread
                                .getId(), Const.ONE, Command.REM, serviceName,
                        ((InetSocketAddress)channelToLeader.localAddress()).getAddress().getHostAddress())));
            }
        }
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


    private void addInstanceLocally(String name, String instance) {
        Set<String> instances;
        if ((instances = registeredInstances.get(name)) == null) {
            synchronized (this) {
                if ((instances = registeredInstances.get(name)) == null) {
                    instances = new CopyOnWriteArraySet<>();
                    instances.add(instance);
                    registeredInstances.put(name, instances);
                } else {
                    instances.add(instance);
                }
            }
        } else {
            instances.add(instance);
        }
    }


    private void remInstanceLocally(String name, String instance) {
        Set<String> instances;
        if ((instances = registeredInstances.get(name)) != null) {
            instances.remove(instance);
        }
    }


    @Override
    public void close() throws Exception {
        isClosed = true;
        channelToLeader.close();
        registeredInstances.clear();
    }
}
