
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

import com.lzp.registry.server.util.Data;
import com.lzp.registry.server.util.DataSearialUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Description:提供写日志的一些api
 *
 * @author: Zeping Lu
 * @date: 2021/3/16 18:41
 */
public class LogService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogService.class);
    private static BufferedWriter bufferedWriter;
    private static BufferedReader bufferedReader;
    private static long index;

    static {
        try {
            long baseCount;
            try (BufferedReader baseIndexReader = new BufferedReader(new FileReader("./persistence/coveredindex.snp"))) {
                baseCount = Long.parseLong(baseIndexReader.readLine());
            }
            bufferedWriter = new BufferedWriter(new FileWriter("./persistence/journal.txt", true));
            bufferedReader = new BufferedReader(new FileReader("./persistence/journal.txt"));
            index = baseCount + bufferedReader.lines().count() - 1;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }


    /**
     * append log and return log index
     */
    public static long append(String command, long term) {
        try {
            bufferedWriter.write(command + " " + term);
            bufferedWriter.newLine();
            bufferedWriter.flush();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return ++index;
    }

    /**
     * 生成快照文件并清空日志文件
     */
    public static void generateSnapshotAndClearJournal(Data data) {
        writeIndex();
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("./persistence/snapshot.snp"))) {
            bufferedOutputStream.write(DataSearialUtil.serialize(data));
            bufferedOutputStream.flush();
            bufferedWriter.close();
            bufferedWriter = new BufferedWriter(new FileWriter("./persistence/journal.txt"));
        } catch (IOException e) {
            LOGGER.error("generate snapshot error", e);
        }
    }

    /**
     * 把快照包含的日志条目持久化到磁盘
     */
    private static void writeIndex() {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("./persistence/coveredindex.snp"))) {
            bufferedWriter.write(Long.toString(index));
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
     * 获取当前raftnode的当前term
     */
    public static String getTerm() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("./persistence/term.txt"))) {
            String currentTerm = bufferedReader.readLine();
            return currentTerm;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return getTerm();
        }
    }



    /**
     * 获取当前日志的index
     */
    public static long getLogIndex() {
        return index;
    }
}
