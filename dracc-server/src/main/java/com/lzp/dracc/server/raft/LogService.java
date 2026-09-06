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

package com.lzp.dracc.server.raft;

import com.lzp.dracc.common.constant.Command;
import com.lzp.dracc.common.constant.Const;
import com.lzp.dracc.common.util.PropertyUtil;
import com.lzp.dracc.common.util.StringUtil;
import com.lzp.dracc.common.util.ThreadFactoryImpl;
import com.lzp.dracc.server.util.Data;
import com.lzp.dracc.server.util.DataSearialUtil;
import com.lzp.dracc.server.util.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Description:提供写日志的一些api
 *
 * 总共有四个文件
 * 1、committedEntry.txt
 * 保存所有已提交的日志条目
 * 2、uncommittedEntry.txt
 * 保存所有未提交的日志条目
 * 3、snapshot.snp
 * 保存状态机快照的文件
 * 4、coveredindex.txt
 * 保存被覆盖的日志索引总数
 *
 * 具体工作流程:
 * 当已提交的日志条目数达到一定数时(可配置),会生成当前状态机的快照,保存到snapshot.snp文件中，
 * 然后清空committedEntry.txt文件,并且把已提交日志的最后一条索引保存到coveredindex.txt文件中。
 *
 * 当重启服务时,会先恢复快照文件,并把已提交的日志条目一条一条执行,从而把状态机恢复到服务停止前的状态。
 * 已提交的日志条目数(文本行数)加上coveredindex.txt里读出来的索引值就是实际所有已提交日志条目数。
 * 未提交的日志也会恢复到内存中
 *
 *
 *
 * 就算每天有一亿条写操作记录到这个节点所属的cluster中,也需要2.5多亿年,索引数目才达到Long.MAX_VALUE。
 * 所以基本不用考虑索引变为负数的情况.
 *
 * 就算这个程序真能跑几亿年。那可以等到索引数快到上限时,或者每隔一亿年,人工介入,暂时停止这个集群服务,修改
 * 这个集群所有节点索引日志条目数(先关停所有节点的服务,然后修改每个节点的coveredindex.txt文件,把覆盖的日志
 * 索引数目减去一个固定值)。
 *
 * @author: Zeping Lu
 * @date: 2021/3/16 18:41
 */
public class LogService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogService.class);

    private static FileOutputStream committedFos;
    private static FileOutputStream uncommittedFos;
    private static BufferedWriter committedEntryWriter;
    private static BufferedWriter uncommittedEntryWriter;
    private static long committedIndex;
    private static final char[] BUFFER_FOR_UNCOMMITTED_ENTRY = new char[50000];
    private static int remaintimeNeedToRem;
    private static int lineSumLatestRead;
    private static Queue<String> uncommittedEntries;
    private static final int SNAPSHOT_BATCH_COUNT;
    private static final ExecutorService SYNC_SCHEDULER = new ThreadPoolExecutor(1, 1, 0,
            new LinkedBlockingQueue<>(), new ThreadFactoryImpl("fsync"));
    private static ExecutorService WRITE_LOG_THREAD = new ThreadPoolExecutor(1, 1, 0,
            new LinkedBlockingQueue<>(), new ThreadFactoryImpl("write uncommited log"));

    private static final BlockingQueue<CompletableFuture> futureQueue = new LinkedBlockingQueue();

    static {
        SNAPSHOT_BATCH_COUNT = Integer.parseInt(PropertyUtil.getProperties(Const.PERSI_PRO).getProperty(Const.SNAPSHOT_BATCH_COUNT, "200000"));
        try {
            committedFos = new FileOutputStream(Const.ROOT_PATH + "persistence/committedEntry.txt", true);
            committedEntryWriter = new BufferedWriter(new OutputStreamWriter(committedFos, UTF_8));
            uncommittedFos = new FileOutputStream(Const.ROOT_PATH + "persistence/uncommittedEntry.txt", true);
            uncommittedEntryWriter = new BufferedWriter(new OutputStreamWriter(uncommittedFos, UTF_8));
            //不用ConcurrentLinkedQueue是因为它的size()方法效率太低
            uncommittedEntries = new LinkedBlockingQueue<>();
            restoreCommittedIndex();
            restoreUncommittedEntry();
            restoreStateMachine();
        } catch (Exception e) {
            LOGGER.error("failed to restore persistent data (if there is no persistent data, it is normal)", e);
        }
        SYNC_SCHEDULER.execute(() -> {
            while (true) {
                try {
                    int queSize = futureQueue.size();
                    synchronized (LogService.class) {
                        // 执行 fsync，将两个文件都刷盘
                        committedEntryWriter.flush();
                        committedFos.getFD().sync();
                        uncommittedEntryWriter.flush();
                        uncommittedFos.getFD().sync();
                    }
                    for (int i = 0; i < queSize; i++) {
                        futureQueue.poll().complete(null);
                    }
                    /*if (queSize > 30) {
                        Thread.sleep(60);
                    } else if (queSize < 5) {
                        Thread.sleep(8);
                    } else {
                        Thread.sleep(20);
                    }*/
                    Thread.sleep(10);
                } catch (Exception e) {
                    //fatalError("Failed to sync log files", e);
                    LOGGER.error(e.getMessage(), e);
                }
            }
        });
    }

    /**
     * 处理致命错误：记录日志并立即终止 JVM
     */
    private static void fatalError(String message, Throwable e) {
        LOGGER.error("FATAL: {} Terminating JVM.", message, e);
        Runtime.getRuntime().halt(1);
    }

    /**
     * 添加未提交的日志条目,并返回添加后未提交的日志条目总数
     */
    public static Object[] appendUnCommittedLog(String command) {
        CompletableFuture<Void> pendingFuture = new CompletableFuture<>();
        uncommittedEntries.offer(command);
        WRITE_LOG_THREAD.execute(() -> {
            try {
                synchronized (LogService.class) {
                    uncommittedEntryWriter.write(command);
                    uncommittedEntryWriter.newLine();
                }
                futureQueue.offer(pendingFuture);
            } catch (IOException e) {
                fatalError("Failed to append uncommitted log to disk.", e);
            }
        });
        return new Object[]{pendingFuture, uncommittedEntries.size()};
    }

    /**
     * 生成快照文件并清空日志文件
     */
    public static void generateSnapshotAndClearJournal(byte[] dataSnapshot, long committedIndex) {
        try (FileOutputStream snapshotFos = new FileOutputStream(Const.ROOT_PATH + "persistence/snapshot.snp")) {
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(snapshotFos);
            bufferedOutputStream.write(dataSnapshot);
            bufferedOutputStream.flush();
            snapshotFos.getFD().sync();
            bufferedOutputStream.close();
            // 关闭旧的 committed writer 和 fos
            committedEntryWriter.close();
            committedFos.close();
            // 重新以覆盖模式打开 committedEntry.txt（清空）
            committedFos = new FileOutputStream(Const.ROOT_PATH + "persistence/committedEntry.txt");
            committedEntryWriter = new BufferedWriter(new OutputStreamWriter(committedFos, UTF_8));
            committedFos.getFD().sync();
        } catch (IOException e) {
            fatalError("Failed to generate snapshot or clear journal.", e);
        }
        writeCoveredIndex(Long.toString(committedIndex));
    }

    /**
     * 提交第一条未提交的日志
     *
     * @return 是否改变了状态机
     */
    public static boolean commitFirstUncommittedLog() {
        String command = LogService.pollFirstUncommittedEntry();
        if (command != null && parseAndExecuteCommand(command)) {
            commitFirstLog(command);
            return true;
        }
        return false;
    }


    /**
     * 获取已提交日志的index
     */
    public static long getCommittedLogIndex() {
        return committedIndex;
    }

    /**
     * 更新当前raftnode的term
     */
    public static void updateCurrentTerm(String newTerm) {
        try (FileOutputStream fos = new FileOutputStream(Const.ROOT_PATH + "persistence/term.txt")) {
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            bos.write(newTerm.getBytes(UTF_8));
            bos.flush();
            fos.getFD().sync();
            bos.close();
        } catch (IOException e) {
            fatalError("Failed to persist term to disk.", e);
        }
    }

    /**
     * 获取当前raftnode的当前term(重启后)
     */
    public static String getTerm() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(Const.ROOT_PATH + "persistence/term.txt"))) {
            String preservedTerm = bufferedReader.readLine();
            return preservedTerm == null ? "0" : preservedTerm;
        } catch (IOException e) {
            LOGGER.error("failed to read term.txt", e);
            return "0";
        }
    }

    /**
     * 清空未提交的日志记录
     */
    public static void clearUncommittedEntry() {
        WRITE_LOG_THREAD.execute(() -> {
            synchronized (LogService.class) {
                try {
                    // 关闭旧资源
                    uncommittedEntryWriter.close();
                    uncommittedFos.close();
                    // 重新以覆盖模式打开
                    uncommittedFos = new FileOutputStream(Const.ROOT_PATH + "persistence/uncommittedEntry.txt");
                    uncommittedEntryWriter = new BufferedWriter(new OutputStreamWriter(uncommittedFos, UTF_8));
                    uncommittedFos.getFD().sync();
                } catch (IOException e) {
                    fatalError("Failed to clear uncommitted log file.", e);
                }
            }
            uncommittedEntries.clear();
        });
    }

    /**
     * 获取未提交日志的条目数
     */
    public static int getUncommittedLogSize() {
        return uncommittedEntries.size();
    }

    /**
     * 获取保存未提交日志的文件的具体内容
     */
    public static byte[] getFileContentOfUncommittedEntry() {
        try (FileInputStream fis = new FileInputStream(Const.ROOT_PATH + "persistence/uncommittedEntry.txt");
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = fis.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            fatalError("Failed to read uncommitted entry file.", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取保存已提交日志的文件的具体内容
     */
    public static byte[] getFileContentOfCommittedEntry() {
        try (FileInputStream fis = new FileInputStream(Const.ROOT_PATH + "persistence/committedEntry.txt");
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = fis.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            fatalError("Failed to read uncommitted entry file.", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取被覆盖的日志条目数
     */
    public static String getCoveredIndex() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(Const.ROOT_PATH + "persistence/coveredindex.txt"))) {
            String coveredIndex;
            return (coveredIndex = bufferedReader.readLine()) == null ? "0" : coveredIndex;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return "0";
        }
    }

    /**
     * 同步未提交日志,用作主从失连恢复后数据同步
     */
    public static void syncUncommittedLog(byte[] uncommittedBytes) {
        WRITE_LOG_THREAD.execute(() -> {
            try {
                synchronized (LogService.class) {
                    uncommittedEntryWriter.close();
                    uncommittedFos.close();
                    uncommittedFos = new FileOutputStream(Const.ROOT_PATH + "persistence/uncommittedEntry.txt");
                    uncommittedEntryWriter = new BufferedWriter(new OutputStreamWriter(uncommittedFos, UTF_8));
                    uncommittedFos.write(uncommittedBytes);
                    uncommittedFos.flush();
                    uncommittedFos.getFD().sync();
                }
                uncommittedEntries.clear();
                restoreUncommittedEntry();
            } catch (IOException e) {
                fatalError("Failed to sync uncommitted log.", e);
            }
        });
    }

    /**
     * 同步已提交日志,用作主从失连恢复后数据同步
     */
    public static void syncCommittedLog(byte[] committedBytes, String coveredIndex) {
        WRITE_LOG_THREAD.execute(() -> {
            try {
                synchronized (LogService.class) {
                    committedEntryWriter.close();
                    committedFos.close();
                    committedFos = new FileOutputStream(Const.ROOT_PATH + "persistence/committedEntry.txt");
                    committedEntryWriter = new BufferedWriter(new OutputStreamWriter(committedFos, UTF_8));
                    committedFos.write(committedBytes);
                    committedFos.flush();
                    committedFos.getFD().sync();
                }
                writeCoveredIndex(coveredIndex);
                restoreCommittedIndex();
            } catch (IOException e) {
                fatalError("Failed to sync committed log.", e);
            }
        });
    }

    /**
     * 保存快照文件
     */
    public static void saveSnapshot(byte[] snapshotBytes) {
        WRITE_LOG_THREAD.execute(() -> {
            try (FileOutputStream snapshotFos = new FileOutputStream(Const.ROOT_PATH + "persistence/snapshot.snp")) {
                snapshotFos.write(snapshotBytes);
                snapshotFos.flush();
                snapshotFos.getFD().sync();
            } catch (IOException e) {
                fatalError("Failed to save snapshot", e);
            }
        });
    }
    /**
     * 把未提交的日志条目恢复到内存中
     */
    private static void restoreUncommittedEntry() throws IOException {
        try (BufferedReader uncommittedEntryReader = new BufferedReader(new FileReader(Const.ROOT_PATH + "persistence/uncommittedEntry.txt"))) {
            String uncommittedEntry;
            while ((uncommittedEntry = uncommittedEntryReader.readLine()) != null) {
                uncommittedEntries.offer(uncommittedEntry);
            }
        }
    }


    /**
     * 恢复已提交日志最后条的索引
     */
    private static void restoreCommittedIndex() throws IOException {
        long baseCount;
        try (BufferedReader committedEntryReader = new BufferedReader(new FileReader(Const.ROOT_PATH + "persistence/committedEntry.txt"))) {
            baseCount = Long.parseLong(getCoveredIndex());
            committedIndex = baseCount + committedEntryReader.lines().count() - 1;
        }
    }

    /**
     * 恢复状态机
     */
    private static void restoreStateMachine() {
        initDataOfRaftNode();
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(Const.ROOT_PATH + "persistence/snapshot.snp"));
             BufferedReader committedEntryReader = new BufferedReader(new FileReader(Const.ROOT_PATH + "persistence/committedEntry.txt"))) {
            byte[] bytes = new byte[bufferedInputStream.available()];
            bufferedInputStream.read(bytes);
            if (bytes.length > 0) {
                RaftNode.data = (Map<String, Object>[]) DataSearialUtil.deserialize(bytes).getObject();
            }
            String command;
            while ((command = committedEntryReader.readLine()) != null && !command.isEmpty()) {
                parseAndExecuteCommand(command);
            }
        } catch (Exception e) {
            LOGGER.error("restore state machine error", e);
        }
    }

    /**
     * 初始化状态机
     */
    private static void initDataOfRaftNode() {
        RaftNode.data[0] = new HashMap<>();
        RaftNode.data[1] = new ConcurrentHashMap<>();
        RaftNode.data[2] = new ConcurrentHashMap<>();
    }

    /**
     * 执行写状态机的具体操作
     *
     * @return 是否改变了状态机
     */
    private static boolean parseAndExecuteCommand(String command) {
        String[] commandDetails = StringUtil.stringSplit(command, Const.SPECIFICORDER_SEPARATOR);
        if (Const.ZERO.equals(commandDetails[0])) {
            return executeCommandForService(commandDetails);
        } else if (Const.ONE.equals(commandDetails[0])) {
            return executeCommandForConfig(commandDetails);
        } else {
            return executeCommandForLock(commandDetails);
        }
    }

    /**
     * 执行针对服务的命令
     *
     * @return 是否改变了状态机
     */
    private static boolean executeCommandForService(String[] commandDetails) {
        Set<String> services;
        boolean operSucceed = false;
        if (Command.ADD.equals(commandDetails[1])) {
            if ((services = (Set<String>) RaftNode.data[0].get(commandDetails[2])) == null) {
                services = new HashSet<>();
                RaftNode.data[0].put(commandDetails[2], services);
            }
            //这里加锁是为了防止健康检查的线程读数据时出问题。写的时候都是单线程的(IO线程)。下面原因相同
            synchronized (services) {
                operSucceed = services.add(commandDetails[3]);
            }
        } else {
            if ((services = (Set<String>) RaftNode.data[0].get(commandDetails[2])) != null) {
                synchronized (services) {
                    operSucceed = services.remove(commandDetails[3]);
                }
                if (services.isEmpty()) {
                    RaftNode.data[0].remove(commandDetails[2]);
                }
            }
        }
        return operSucceed;
    }

    /**
     * 执行针对配置的命令
     *
     * @return 是否改变了状态机
     */
    private static boolean executeCommandForConfig(String[] commandDetails) {
        Set<String> configs;
        if (Command.ADD.equals(commandDetails[1])) {
            if ((configs = (Set<String>) RaftNode.data[1].get(commandDetails[2])) == null) {
                configs = new HashSet<>();
                RaftNode.data[1].put(commandDetails[2], configs);
            }
            return configs.add(commandDetails[3]);
        } else {
            if ((configs = (Set<String>) RaftNode.data[1].get(commandDetails[2])) != null) {
                return configs.remove(commandDetails[3]);
            }
            return false;
        }
    }

    /**
     * 获取快照文件
     *
     * @return 快照文件字节数组
     */
    public static byte[] getFileContentOfSnapshot() {
        try (FileInputStream fis = new FileInputStream(Const.ROOT_PATH + "persistence/snapshot.snp");
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = fis.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            LOGGER.error("Failed to read snapshot file", e);
            return new byte[0]; // 或返回 null，调用方处理
        }
    }

    /**
     * 执行针对锁的命令
     * @return 是否改变了状态机
     */
    private static boolean executeCommandForLock(String[] commandDetails) {
        if (Command.ADD.equals(commandDetails[1])) {
            List<String> locks;
            if ((locks = (List<String>) RaftNode.data[2].get(commandDetails[2])) == null) {
                locks = new LinkedList<>();
                RaftNode.data[2].put(commandDetails[2], locks);
                return locks.add(commandDetails[3]);
            } else if (locks.size() == 0) {
                locks.add(commandDetails[3]);
            } else if (!locks.contains(commandDetails[3])) {
                return locks.add(commandDetails[3]);
            }
        } else {
            LinkedList<String> locks;
            //只有当前持有锁才有释放锁的权力
            if ((locks = (LinkedList<String>) RaftNode.data[2].get(commandDetails[2])) != null &&
                    commandDetails[3].equals(locks.getFirst())) {
                locks.removeFirst();
            }
        }
        return false;
    }

    /**
     * 添加已提交的日志条目
     */
    public static void appendCommittedLog(String command, long committedIndex, byte[] dataSnapshot) {
        try {
            synchronized (LogService.class) {
                committedEntryWriter.write(command);
                committedEntryWriter.newLine();
                if (dataSnapshot != null) {
                    generateSnapshotAndClearJournal(dataSnapshot, committedIndex);
                }
            }
        } catch (IOException e) {
            fatalError("Failed to append committed log.", e);
        }
    }

    /**
     * 把快照包含的日志条目持久化到磁盘
     */
    private static void writeCoveredIndex(String coveredIndex) {
        try (FileOutputStream fos = new FileOutputStream(Const.ROOT_PATH + "persistence/coveredindex.txt")) {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
            writer.write(coveredIndex);
            writer.flush();
            fos.getFD().sync();   // 强制落盘
            writer.close();
        } catch (IOException e) {
            fatalError("Failed to write covered index.", e);
        }
    }

    /**
     * 把保存未提交日志的文件第一行删除,并返回内容
     * <p>
     * TODO
     * 实现逻辑是通过重写未提交日志文件来删除第一行,不过做过优化,如果未提交日志有多行(说明客户端在高并发写数据)，会批量删的。
     * <p>
     * 还有一种比较好的实现逻辑是，通过引入一个持久化的偏移量（offset）来避免每次删除首行时重写整个文件。
     * 这个偏移量记录文件中有效未提交日志的起始位置（字节偏移或字符偏移）。删除第一行时，
     * 只需更新偏移量到下一行的起始位置即可。当偏移量超过一定阈值时，再对文件进行压缩（重写有效部分并重置偏移量）。
     * 不过这种实现逻辑要引入更多的变量以及持久化文件，复杂度比较高，而且性能也不会比我现在的高，就不改了。
     */
    public static void removeFirstUncommittedEntry() {
        // 如果内存队列为空，说明所有未提交日志都已提交，直接清空文件
        if (uncommittedEntries.isEmpty()) {
            synchronized (LogService.class) {
                try {
                    clearUncommitedLog();
                } catch (IOException e) {
                    fatalError("Failed to clear uncommitted log file.", e);
                }
            }
        }
    }

    /**
     * 清空未提交日志
     */
    private static void clearUncommitedLog() throws IOException {
        // 关闭旧资源
        uncommittedEntryWriter.close();
        uncommittedFos.close();
        // 重新以覆盖模式打开
        uncommittedFos = new FileOutputStream(Const.ROOT_PATH + "persistence/uncommittedEntry.txt");
        uncommittedEntryWriter = new BufferedWriter(new OutputStreamWriter(uncommittedFos, UTF_8));
        //写到操作系统缓存就行了,如果还没落盘就宕机,没影响,写操作全是幂等操作,已提交日志已经写入就行
        uncommittedEntryWriter.flush();
    }

    /**
     * 获取写日志的线程池
     */
    public static void waitUntilAllLogWriteComplete() {
        WRITE_LOG_THREAD.shutdown();
        try {
            WRITE_LOG_THREAD.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fatalError(e.getMessage(), e);
        }
        WRITE_LOG_THREAD = new ThreadPoolExecutor(1, 1, 0, new LinkedBlockingQueue<>(),
                new ThreadFactoryImpl("write uncommited log"));
    }

    /**
     * 取出内存队列中的第一条未提交日志
     */
    public static String pollFirstUncommittedEntry() {
        return uncommittedEntries.poll();
    }

    /**
     * 把第一条未提交日志提交,在调用这个方法之前,调用方已经提前取出内存队列中的第一条未提交日志，然后更改状态机
     * 了(提交日志时，未提交日志肯定已经在半数以上节点上落盘了，所以能直接改状态机)，所以这里能直接把参数传入。
     */
    public static void commitFirstLog(String firstLog) {
        long newCommittedIndex = ++committedIndex;
        byte[] dataSnapshot;
        if (committedIndex % SNAPSHOT_BATCH_COUNT == 0) {
            dataSnapshot = DataSearialUtil.serialize(new Data(RaftNode.data));
        } else {
            dataSnapshot = null;
        }
        WRITE_LOG_THREAD.execute(() -> {
            //先添加已提交日志，再删除未提交日志(落盘线程也是先落已提交日志文件盘),这样已提交日志写入后突然断电,也不会丢失数据。
            appendCommittedLog(firstLog, newCommittedIndex, dataSnapshot);
            removeFirstUncommittedEntry();
        });
    }

}