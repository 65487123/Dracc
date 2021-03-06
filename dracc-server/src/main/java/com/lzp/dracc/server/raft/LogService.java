
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
import com.lzp.dracc.server.util.Data;
import com.lzp.dracc.server.util.DataSearialUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

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


    private static BufferedWriter committedEntryWriter;
    private static BufferedWriter uncommittedEntryWriter;
    private static long committedIndex;
    private static final char[] BUFFER_FOR_UNCOMMITTED_ENTRY = new char[50000];
    private static Queue<String> uncommittedEntries;
    private static final int SNAPSHOT_BATCH_COUNT;

    static {
        SNAPSHOT_BATCH_COUNT = Integer.parseInt(PropertyUtil.getProperties(Const.PERSI_PRO).getProperty(Const.SNAPSHOT_BATCH_COUNT, "200000"));
        try {
            committedEntryWriter = new BufferedWriter(new FileWriter(Const.ROOT_PATH + "persistence/committedEntry.txt", true));
            uncommittedEntryWriter = new BufferedWriter(new FileWriter(Const.ROOT_PATH + "persistence/uncommittedEntry.txt", true));
            //不用ConcurrentLinkedQueue是因为它的size()方法效率太低
            uncommittedEntries = new ArrayBlockingQueue<>(1000);
            restoreCommittedIndex();
            restoreUncommittedEntry();
            restoreStateMachine();
        } catch (Exception e) {
            LOGGER.error("failed to restore persistent data (if there is no persistent data, it is normal)");
        }
    }


    /**
     * 添加未提交的日志条目,并返回添加后未提交的日志条目总数
     */
    public static long appendUnCommittedLog(String command) {
        try {
            uncommittedEntryWriter.write(command);
            uncommittedEntryWriter.newLine();
            uncommittedEntryWriter.flush();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        uncommittedEntries.offer(command);
        return uncommittedEntries.size();
    }

    /**
     * 生成快照文件并清空日志文件
     */
    public static void generateSnapshotAndClearJournal(Data data) {
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("./persistence/snapshot.snp"))) {
            bufferedOutputStream.write(DataSearialUtil.serialize(data));
            bufferedOutputStream.flush();
            committedEntryWriter.close();
            committedEntryWriter = new BufferedWriter(new FileWriter(Const.ROOT_PATH + "persistence/committedEntry.txt"));
        } catch (IOException e) {
            LOGGER.error("generate snapshot error", e);
        }
        writeCoveredIndex(Long.toString(committedIndex));
    }

    /**
     * 提交第一条未提交的日志
     *
     * @return 是否改变了状态机
     */
    public static boolean commitFirstUncommittedLog() {
        String command = removeFirstUncommittedEntry();
        if (parseAndExecuteCommand(command)) {
            appendCommittedLog(command);
            return true;
        }
        return false;
    }

    /**
     * 提交所有未提交的日志
     * <p>
     * 当成功竞选为主后,发现还有未提交的日志,会执行这个方法
     */
    public static void commitAllUncommittedLog() {
        while (!uncommittedEntries.isEmpty()) {
            commitFirstUncommittedLog();
        }
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
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(Const.ROOT_PATH + "persistence/term.txt"))) {
            bufferedOutputStream.write(newTerm.getBytes(UTF_8));
            bufferedOutputStream.flush();
        } catch (IOException e) {
            updateCurrentTerm(newTerm);
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
        try {
            uncommittedEntryWriter = new BufferedWriter(new FileWriter(Const.ROOT_PATH + "persistence/uncommittedEntry.txt"));
            uncommittedEntries.clear();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            clearUncommittedEntry();
        }
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
    public static String getFileContentOfUncommittedEntry() {
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(Const.ROOT_PATH + "persistence/uncommittedEntry.txt"))) {
            byte[] bytes = new byte[bufferedInputStream.available()];
            bufferedInputStream.read(bytes);
            return new String(bytes, UTF_8);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return "null";
        }
    }

    /**
     * 获取保存已提交日志的文件的具体内容
     */
    public static String getFileContentOfCommittedEntry() {
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(Const.ROOT_PATH + "persistence/committedEntry.txt"))) {
            byte[] bytes = new byte[bufferedInputStream.available()];
            bufferedInputStream.read(bytes);
            return new String(bytes, UTF_8);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return "null";
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
    public static void syncUncommittedLog(String uncommittedLog) {
        try {
            uncommittedEntryWriter = new BufferedWriter(new FileWriter(Const.ROOT_PATH + "persistence/uncommittedEntry.txt"));
            uncommittedEntryWriter.write(uncommittedLog);
            uncommittedEntries.clear();
            restoreUncommittedEntry();
        } catch (IOException e) {
            LOGGER.error("write uncommitted log failed", e);
        }
    }

    /**
     * 同步已提交日志,用作主从失连恢复后数据同步
     */
    public static void syncCommittedLog(String committedLog, String coveredIndex) {
        try {
            committedEntryWriter = new BufferedWriter(new FileWriter(Const.ROOT_PATH + "persistence/committedEntry.txt"));
            committedEntryWriter.write(committedLog);
            writeCoveredIndex(coveredIndex);
            restoreCommittedIndex();
        } catch (IOException e) {
            LOGGER.error("write uncommitted log failed", e);
        }
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
     * 删除第一行记录
     *
     * @param chars   保存字符的空间
     * @param numRead 有效字符个数(从文件中读出来的总字符个数)
     * @return 删除第一行记录后的总字符个数
     */
    private static int removeTheFirstLine(char[] chars, int numRead) {
        int beginIndex = 0;
        for (int i = 6; i < chars.length; i++) {
            if (chars[i] == '\n' || (chars[i] == '\r' && chars[i + 1] != '\n')) {
                beginIndex = i + 1;
                break;
            }
        }
        int numMoved = numRead - beginIndex;
        System.arraycopy(chars, beginIndex, chars, 0, numMoved);
        return numMoved;
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
        try (BufferedInputStream bufferedOutputStream = new BufferedInputStream(new FileInputStream(Const.ROOT_PATH + "persistence/snapshot.snp"));
             BufferedReader committedEntryReader = new BufferedReader(new FileReader(Const.ROOT_PATH + "persistence/committedEntry.txt"))) {
            byte[] bytes = new byte[bufferedOutputStream.available()];
            bufferedOutputStream.read(bytes);
            if (bytes.length>0) {
                RaftNode.data = (Map<String, Object>[]) DataSearialUtil.deserialize(bytes).getObject();
            }
            String command;
            while ((command = committedEntryReader.readLine()) != null) {
                parseAndExecuteCommand(command);
            }
        } catch (Exception e) {
            LOGGER.error("restore state machine error", e);
        }
    }

    /**
     * 初始化状态机
     */
    private static void initDataOfRaftNode(){
        RaftNode.data[0] = new HashMap<>();
        RaftNode.data[1] = new ConcurrentHashMap<>();
        RaftNode.data[2] = new ConcurrentHashMap<>();
    }


    /**
     * 执行写状态机的具体操作
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
    public static void appendCommittedLog(String command) {
        try {
            committedEntryWriter.write(command);
            committedEntryWriter.newLine();
            committedEntryWriter.flush();
            if (++committedIndex % SNAPSHOT_BATCH_COUNT == 0) {
                generateSnapshotAndClearJournal(new Data(RaftNode.data));
            }
        } catch (IOException e) {
            LOGGER.error("append committed log failed", e);
        }
    }

    /**
     * 把快照包含的日志条目持久化到磁盘
     */
    private static void writeCoveredIndex(String coveredIndex) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(Const.ROOT_PATH + "persistence/coveredindex.txt"))) {
            bufferedWriter.write(coveredIndex);
            bufferedWriter.flush();
        } catch (IOException e) {
            LOGGER.error("generate snapshot error", e);
        }
    }

    /**
     * 把保存未提交日志的文件第一行删除,并返回内容
     */
    public static String removeFirstUncommittedEntry() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(Const.ROOT_PATH + "persistence/uncommittedEntry.txt"))) {
            int num = removeTheFirstLine(BUFFER_FOR_UNCOMMITTED_ENTRY, bufferedReader.read(BUFFER_FOR_UNCOMMITTED_ENTRY));
            uncommittedEntryWriter = new BufferedWriter(new FileWriter(Const.ROOT_PATH + "persistence/uncommittedEntry.txt"));
            uncommittedEntryWriter.write(BUFFER_FOR_UNCOMMITTED_ENTRY, 0, num);
            uncommittedEntryWriter.flush();
            return uncommittedEntries.poll();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return null;
    }

}
