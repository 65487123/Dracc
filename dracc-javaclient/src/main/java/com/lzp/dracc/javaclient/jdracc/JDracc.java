
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

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * Description:Dracc java客户端实现
 *
 *
 * @author: Zeping Lu
 * @date: 2021/3/24 19:48
 */
public class JDracc implements DraccClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDracc.class);

    /**
     * jvm的进程号,用分布式锁时会用到
     */
    private static final long JVM_PID;

    /**
     * 本客户端是否已关闭标识
     */
    private volatile boolean isClosed = false;

    /**
     * 和主节点的连接
     */
    private Channel channelToLeader;

    /**
     * rpc超时时间,单位是ms
     */
    private final int TIMEOUT;

    /**
     * 在本地保存一份通过这个客户端注册的所有实例,当与server失连并且重连后,重新再注册一遍
     * 由于注册实例是幂等操作,server端就算还是存在这个实例,也不会有影响
     */
    private final Map<String, Set<String>> REGISTERED_INSTANCES = new ConcurrentHashMap<>();


    static {
        String jvmName;
        JVM_PID = Long.parseLong((jvmName = ManagementFactory.getRuntimeMXBean().getName()).substring(0, jvmName.indexOf('@')));
    }

    /**
     * 创建一个Dracc集群的客户端
     *
     * @param timeout    读写dracc的超时时间,单位是毫秒,超过这个时间没响应会抛出异常
     * @param ipAndPorts dracc集群所有节点的ip及端口
     * @throws DraccException exception
     */
    public JDracc(int timeout, String... ipAndPorts) throws InterruptedException, DraccException {
        setUpChannelToLeader(ipAndPorts);
        this.TIMEOUT = timeout;
    }

    /**
     * 创建一个Dracc集群的客户端
     *
     * @param ipAndPorts dracc集群所有节点的ip及端口
     * @throws DraccException exception
     */
    public JDracc(String... ipAndPorts) throws InterruptedException, DraccException {
        this(5000, ipAndPorts);
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
                    HeartbeatWorker.executeHeartBeat(channelToLeader.closeFuture()
                            .addListener(future -> onChannelClosed(ipAndPorts)).channel());
                }
            } else {
                throw new DraccException("can not find leader");
            }
        } finally {
            threadPool.shutdownNow();
        }
    }


    private void onChannelClosed(String... ipAndPorts) {
        HeartbeatWorker.stopHeartBeat(channelToLeader);
        if (resetChannelIfNecessary(ipAndPorts)) {
            try {
                for (Map.Entry<String, Set<String>> entry : REGISTERED_INSTANCES.entrySet()) {
                    for (String instance : entry.getValue()) {
                        registerInstance0(entry.getKey(), instance);
                    }
                }
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * 如果客户端没关闭,重新和server建立连接
     *
     * @return 是否成功和server建立连接
     */
    private synchronized boolean resetChannelIfNecessary(String... ipAndPorts) {
        try {
            if (!isClosed) {
                Thread.sleep(1000);
                LOGGER.warn("Lost connection with the leader or reset ChannelToleader failed, set ChannelToleader again");
                setUpChannelToLeader(ipAndPorts);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            return resetChannelIfNecessary(ipAndPorts);
        }
    }


    private void findLeaderAndSetChannel(String ipAndPort, CountDownLatch countDownLatch) {
        Channel channel = ConnectionFactory.newChannel(ipAndPort);
        Thread thisThread = Thread.currentThread();
        ResultHandler.ThreadResultAndTime threadResultAndTime = new ResultHandler.ThreadResultAndTime(System.currentTimeMillis() + 5000, thisThread);
        ResultHandler.reqIdThreadMap.put(thisThread.getName(), threadResultAndTime);
        channel.writeAndFlush((thisThread.getName() + Const.COMMAND_SEPARATOR + Const.RPC_GETROLE).getBytes(StandardCharsets.UTF_8));
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


    private String sentRpcAndGetResult(String commandId, Thread currentThread, String command, long timeout) {
        System.out.println(command);
        ResultHandler.ThreadResultAndTime threadResultAndTime = new ResultHandler
                .ThreadResultAndTime(System.currentTimeMillis() + timeout, currentThread);
        ResultHandler.reqIdThreadMap.put(commandId, threadResultAndTime);
        channelToLeader.writeAndFlush(command.getBytes(StandardCharsets.UTF_8));
        String result;
        while ((result = threadResultAndTime.getResult()) == null) {
            LockSupport.park();
        }
        return result;
    }


    private String sentRpcAndGetResult(String commandId, Thread currentThread, String command) {
        return sentRpcAndGetResult(commandId, currentThread, command, TIMEOUT);
    }


    private String generateCommand(String commandId, String dataType, String operType, String key, String value) {
        return commandId + Const.COMMAND_SEPARATOR + Const.RPC_FROMCLIENT + Const.COMMAND_SEPARATOR
                + dataType + Const.COMMAND_SEPARATOR + operType + Const.COMMAND_SEPARATOR
                + key + Const.COMMAND_SEPARATOR + value;
    }



    private String genCmdForGet(String commandId, String dataType, String key) {
        return commandId + Const.COMMAND_SEPARATOR + Const.RPC_FROMCLIENT + Const.COMMAND_SEPARATOR
                + dataType + Const.COMMAND_SEPARATOR + Command.GET + Const.COMMAND_SEPARATOR + key;
    }


    private String checkResult(String result) throws DraccException {
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
        return result;
    }


    @Override
    public void registerInstance(String serviceName, String ip, int port) throws DraccException {
        registerInstance0(serviceName, ip + Const.COLON + port);
    }


    private void registerInstance0(String serviceName, String instance) throws DraccException {
        Thread currentThread;
        String threadName = (currentThread = Thread.currentThread()).getName();
        checkResult(sentRpcAndGetResult(threadName, currentThread, generateCommand(threadName,
                Const.ZERO, Command.ADD, serviceName, instance)));
        addInstanceLocally(serviceName, instance);
    }


    @Override
    public void deregisterInstance(String serviceName, String ip, int port) throws DraccException {
        deregisterInstance0(serviceName, ip + Const.COLON + port);
    }

    public void deregisterInstance0(String serviceName, String instance) throws DraccException {
        Thread currentThread;
        String threadName = (currentThread = Thread.currentThread()).getName();
        checkResult(sentRpcAndGetResult(threadName, currentThread, generateCommand(threadName,
                Const.ZERO, Command.REM, serviceName, instance)));
        remInstanceLocally(serviceName, instance);
    }

    @Override
    public List<String> getAllInstances(String serviceName) throws DraccException {
        Thread currentThread;
        String threadName = (currentThread = Thread.currentThread()).getName();
        String result = sentRpcAndGetResult(threadName, currentThread, genCmdForGet(threadName,
                Const.ZERO, serviceName));
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
        Thread currentThread;
        String threadName = (currentThread = Thread.currentThread()).getName();
        checkResult(sentRpcAndGetResult(threadName, currentThread, generateCommand(threadName, Const.ONE,
                Command.ADD, serviceName, ((InetSocketAddress) channelToLeader.localAddress())
                        .getAddress().getHostAddress())));
    }


    @Override
    public void unsubscribe(String serviceName, EventListener listener) throws DraccException {
        Set<EventListener> eventListeners;
        if ((eventListeners = ResultHandler.serviceNameListenerMap.get(serviceName)) != null) {
            eventListeners.remove(listener);
            if (eventListeners.size() == 0) {
                Thread currentThread;
                String threadName = (currentThread = Thread.currentThread()).getName();
                checkResult(sentRpcAndGetResult(threadName, currentThread, generateCommand(threadName,
                        Const.ONE, Command.REM, serviceName, ((InetSocketAddress) channelToLeader
                                .localAddress()).getAddress().getHostAddress())));
            }
        }
    }


    @Override
    public void addConfig(String configName, String configVal) throws DraccException {
        Thread currentThread;
        String threadName = (currentThread = Thread.currentThread()).getName();
        checkResult(sentRpcAndGetResult(threadName, currentThread,
                generateCommand(threadName, Const.ONE, Command.ADD, configName, configVal)));
    }


    @Override
    public String removeConfig(String configName, String configVal) throws DraccException {
        Thread currentThread;
        String threadName = (currentThread = Thread.currentThread()).getName();
        return checkResult(sentRpcAndGetResult(threadName, currentThread,
                generateCommand(threadName, Const.ONE, Command.REM, configName, configVal)));
    }


    @Override
    public List<String> getConfig(String configName) throws DraccException {
        Thread currentThread;
        String threadName = (currentThread = Thread.currentThread()).getName();
        String result = sentRpcAndGetResult(threadName, currentThread,
                genCmdForGet(threadName, Const.ONE, configName));
        return CommonUtil.deserial(checkResult(result));
    }


    @Override
    public void acquireDistributedLock(String lockName) {
        Thread currentThread;
        String threadName = (currentThread = Thread.currentThread()).getName();
        try {
            checkResult(sentRpcAndGetResult(JVM_PID + threadName, currentThread,
                    generateCommand(threadName, Const.TWO, Command.ADD, lockName,
                            ((InetSocketAddress) channelToLeader.localAddress())
                                    .getAddress().getHostAddress()), TIMEOUT));
        } catch (DraccException e) {
            acquireDistributedLock(lockName);
        }
    }


    @Override
    public void releaseDistributedlock(String lockName) throws DraccException {
        Thread currentThread;
        String threadName = (currentThread = Thread.currentThread()).getName();
        try {
            checkResult(sentRpcAndGetResult(JVM_PID + threadName, currentThread,
                    generateCommand(threadName, Const.TWO, Command.REM, lockName,
                            ((InetSocketAddress) channelToLeader.localAddress())
                                    .getAddress().getHostAddress()), TIMEOUT));
        } catch (DraccException e) {
            acquireDistributedLock(lockName);
        }
    }


    private void addInstanceLocally(String name, String instance) {
        Set<String> instances;
        if ((instances = REGISTERED_INSTANCES.get(name)) == null) {
            synchronized (this) {
                if ((instances = REGISTERED_INSTANCES.get(name)) == null) {
                    instances = new CopyOnWriteArraySet<>();
                    instances.add(instance);
                    REGISTERED_INSTANCES.put(name, instances);
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
        if ((instances = REGISTERED_INSTANCES.get(name)) != null) {
            instances.remove(instance);
        }
    }

    /**
     * 这个方法谨慎调用
     * 如果是共用用的客户端
     * 确保:
     * 1、在调用这个方法时,没有其他线程在用这个客户端。
     * 2、其他地方后面不可能再用到这个客户端。(为了性能,通过客户端读写server数据时没有校验是否已关闭)
     */
    @Override
    public synchronized void close() throws Exception {
        isClosed = true;
        channelToLeader.close();
        REGISTERED_INSTANCES.clear();
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }


}
