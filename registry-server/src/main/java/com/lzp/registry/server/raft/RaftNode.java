
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

package com.lzp.registry.server.raft;

import com.lzp.registry.common.constant.Cons;
import com.lzp.registry.common.util.PropertyUtil;
import com.lzp.registry.common.util.ThreadFactoryImpl;
import com.lzp.registry.server.netty.NettyClient;
import com.lzp.registry.server.netty.NettyServer;
import com.lzp.registry.server.util.CountDownLatch;
import io.netty.channel.Channel;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Description:raft节点
 *
 * @author: Zeping Lu
 * @date: 2021/3/16 16:45
 */
public class RaftNode {

    private static class DelayTask implements Delayed,Runnable{

        Runnable runnable;
        long deadline;
        long delay;

        DelayTask(Runnable runnable, long delay) {
            this.runnable = runnable;
            this.deadline = System.currentTimeMillis() + (this.delay = delay);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public void run() {
            runnable.run();
        }
    }

    /**
     * 真正的数据(状态机)
     */
    private static Map<String, Set<String>> data = new HashMap<>();

    /**
     * 任期
     */
    private static long term;

    /**
     * 当前节点角色
     */
    private static volatile String role;


    /**
     * 执行超时选举任务的线程池
     */
    private static ExecutorService timeoutToElectionExecutor;

    /**
     * 超时选举任务
     */
    private static DelayTask electionTask;

    /**
     * 主节点才有值
     */
    private static List<Channel> slaveChannels;

    /**
     * 半数
     */
    private static short halfCount;

    /**
     * key是commandid,value是这个command向从节点发送以后收到的结果(是否半数成功)
     */
    private static Map<String, CountDownLatch> cidAndResultMap = new HashMap<>();

    /**
     * id计数器
     */
    private static AtomicInteger commandIdCunter = new AtomicInteger();

    static {
        Properties clusterProperties = PropertyUtil.getProperties(Cons.CLU_PRO);
        String[] remoteNodeIps = clusterProperties.getProperty("remoteRaftNodes").split(",");
        halfCount = (short) (remoteNodeIps.length % 2 == 0 ? remoteNodeIps.length / 2 : remoteNodeIps.length / 2 + 1);
        role = Cons.FOLLOWER;
        String[] localIpAndPort = clusterProperties.getProperty("localRaftNode").split(Cons.COLON);
        NettyServer.start(localIpAndPort[0], Integer.parseInt(localIpAndPort[1]));
        timeoutToElectionExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new DelayQueue(),
                new ThreadFactoryImpl("timeout to election"));
        timeoutToElectionExecutor.execute(() -> {});
        electionTask = new DelayTask(() -> startElection(remoteNodeIps), ThreadLocalRandom.current().nextInt(12000, 18000));
        timeoutToElectionExecutor.execute(electionTask);
    }


    private static void startElection(String[] remoteNodeIps) {
        role = Cons.CANDIDATE;
        String voteRequestId = getCommandId();
        cidAndResultMap.put(voteRequestId, new CountDownLatch(halfCount));
        ExecutorService threadPoolExecutor = new ThreadPoolExecutor(remoteNodeIps.length, remoteNodeIps.length, 0,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                new ThreadFactoryImpl("ask for vote"));
        for (int i = 0; i < remoteNodeIps.length; i++) {
            String[] ipAndPort = remoteNodeIps[i].split(Cons.COLON);
            NettyClient.getChannelAndRequestToVote(ipAndPort[0], Integer.parseInt(ipAndPort[1]), term = LogService.increaseCurrentTerm(term), LogService.getLogIndex());
        }

    }

    public static String getRole() {
        return role;
    }

    public static void setRole(String newRole) {
        role = newRole;
    }

    public static void ResetTimer() {
        electionTask.deadline = System.currentTimeMillis() + electionTask.delay;
    }

    public static String getCommandId() {
        return Integer.toString(commandIdCunter.getAndIncrement());
    }

    public static Map<String, CountDownLatch> getCidAndResultMap() {
        return cidAndResultMap;
    }

}
