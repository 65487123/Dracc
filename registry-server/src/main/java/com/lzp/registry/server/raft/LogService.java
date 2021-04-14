
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
import com.lzp.registry.server.util.Data;
import com.lzp.registry.server.util.DataSearialUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

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

    static {
        try {
            committedEntryWriter = new BufferedWriter(new FileWriter("./persistence/committedEntry.txt", true));
            committedEntryWriter = new BufferedWriter(new FileWriter("./persistence/committedEntry.txt", true));
            restoreCommittedIndex();
            restoreUncommittedEntry();
            restoreStateMachine();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * 恢复状态机
     */
    private static void restoreStateMachine() {
        try (BufferedInputStream bufferedOutputStream = new BufferedInputStream(new FileInputStream("./persistence/snapshot.snp"));
             BufferedReader committedEntryReader = new BufferedReader(new FileReader("./persistence/committedEntry.txt"))) {
            byte[] bytes = new byte[bufferedOutputStream.available()];
            bufferedOutputStream.read(bytes);
            RaftNode.data = (Map<String, Set<String>>) DataSearialUtil.deserialize(bytes).getObject();
            String command;
            while ((command = committedEntryReader.readLine()) != null) {
                parseAndExecuteCommand(command);
            }
        } catch (IOException e) {
            RaftNode.data = new HashMap<>(100000);
            LOGGER.error("generate snapshot error", e);
        }
    }


    /**
     * 执行写状态机的具体操作
     */
    private static void parseAndExecuteCommand(String command) {
        String[] commandDetails = command.split(Cons.SPECIFICORDER_SEPARATOR);
        Set<String> set;
        if (Cons.ADD.equals(commandDetails[0])) {
            if ((set = RaftNode.data.get(commandDetails[1])) == null) {
                set = new HashSet<>();
            }
            set.add(commandDetails[2]);
        } else {
            if ((set = RaftNode.data.get(commandDetails[1])) == null) {
                set = new HashSet<>();
            }
            set.remove(commandDetails[2]);
        }
    }

    /**
     * 添加已提交的日志条目
     */
    private static void appendCommittedLog(String command) {
        try {
            committedEntryWriter.write(command);
            committedEntryWriter.newLine();
            committedEntryWriter.flush();
            ++committedIndex;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
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
        writeIndex();
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("./persistence/snapshot.snp"))) {
            bufferedOutputStream.write(DataSearialUtil.serialize(data));
            bufferedOutputStream.flush();
            committedEntryWriter.close();
            committedEntryWriter = new BufferedWriter(new FileWriter("./persistence/committedEntry.txt"));
        } catch (IOException e) {
            LOGGER.error("generate snapshot error", e);
        }
    }

    /**
     * 提交第一条未提交的日志
     */
    public static void commitFirstUncommittedLog() {
        removeFirstUncommittedEntry();
        appendCommittedLog(uncommittedEntries.poll());
    }


    /**
     * 获取当前日志的index
     */
    public static long getCommittedLogIndex() {
        return committedIndex;
    }

    /**
     * 把快照包含的日志条目持久化到磁盘
     */
    private static void writeIndex() {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("./persistence/coveredindex.txt"))) {
            bufferedWriter.write(Long.toString(committedIndex));
            bufferedWriter.flush();
        } catch (IOException e) {
            LOGGER.error("generate snapshot error", e);
        }
    }

    /**
     * 更新当前raftnode的term并返回更新后的值
     */
    public static long increaseCurrentTerm(long currentTerm) {
        long newTerm = ++currentTerm;
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("./persistence/term.txt"))) {
            bufferedOutputStream.write(Long.toString(newTerm).getBytes());
            bufferedOutputStream.flush();
            return newTerm;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return increaseCurrentTerm(currentTerm);
        }
    }

    /**
     * 获取当前raftnode的当前term(重启后)
     */
    public static String getTerm() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("./persistence/term.txt"))) {
            return bufferedReader.readLine();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return getTerm();
        }
    }

    /**
     * 把保存未提交日志的文件第一行删除
     */
    private static void removeFirstUncommittedEntry() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("./persistence/uncommittedEntry.txt"))) {
            int num = removeTheFirstLine(BUFFER_FOR_UNCOMMITTED_ENTRY, bufferedReader.read(BUFFER_FOR_UNCOMMITTED_ENTRY));
            uncommittedEntryWriter = new BufferedWriter(new FileWriter("./persistence/uncommittedEntry.txt"));
            uncommittedEntryWriter.write(BUFFER_FOR_UNCOMMITTED_ENTRY, 0, num);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * 清空未提交的日志记录
     */
    public static void clearUncommittedEntry() {
        try {
            uncommittedEntryWriter = new BufferedWriter(new FileWriter("A.txt"));
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
     * 删除第一行记录
     *
     * @param chars   保存字符的空间
     * @param numRead 有效字符个数(从文件中读出来的总字符个数)
     * @return 删除第一行记录后的总字符个数
     */
    private static int removeTheFirstLine(char[] chars,int numRead) {
        int index = 0;
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] == '\r' || chars[i] == '\n') {
                index = i;
                break;
            }
        }
        int biginIndex = index + 1;
        if (chars[biginIndex] == '\n') {
            biginIndex = index + 2;
        }
        int numMoved = numRead - biginIndex;
        System.arraycopy(chars, biginIndex, chars, 0, numMoved);
        return numMoved;
    }

    /**
     * 恢复已提交日志最后条的索引
     */
    private static void restoreCommittedIndex() throws IOException {
        long baseCount;
        try (BufferedReader baseIndexReader = new BufferedReader(new FileReader("./persistence/coveredindex.txt"));
             BufferedReader committedEntryReader = new BufferedReader(new FileReader("./persistence/committedEntry.txt"))) {
            baseCount = Long.parseLong(baseIndexReader.readLine());
            committedIndex = baseCount + committedEntryReader.lines().count() - 1;
        }
    }

    /**
     * 把未提交的日志条目恢复到内存中
     */
    private static void restoreUncommittedEntry() throws IOException {
        //不用ConcurrentLinkedQueue是因为它的size()方法效率太低
        uncommittedEntries = new ArrayBlockingQueue<>(1000);
        try (BufferedReader uncommittedEntryReader = new BufferedReader(new FileReader("./persistence/uncommittedEntry.txt"))) {
            String uncommittedEntry;
            while ((uncommittedEntry = uncommittedEntryReader.readLine()) != null) {
                uncommittedEntries.offer(uncommittedEntry);
            }
        }
    }
}
