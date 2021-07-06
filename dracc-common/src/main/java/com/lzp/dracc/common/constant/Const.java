
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

package com.lzp.dracc.common.constant;

import com.lzp.dracc.common.util.PropertyUtil;

/**
 * Description:常量类
 *
 * @author: Zeping Lu
 * @date: 2021/3/17 16:26
 */
public class Const {
    public static final char COMMA = ',';
    public static final char COLON = ':';
    public static final String COPY_LOG_REQ = "cp_req";
    public static final String COPY_LOG_REPLY = "cp_reply";
    public static final char COMMAND_SEPARATOR = '~';
    public static final char SPECIFICORDER_SEPARATOR = '`';
    public static final String CLU_PRO = "cluster.properties";
    public static final String PERSI_PRO = "persistence.properties";
    public static final String SNAPSHOT_BATCH_COUNT = "snapshot-batch-count";
    public static final String RPC_ASKFORVOTE = "reqforvote";
    public static final String RPC_GETROLE = "getrole";
    public static final String RPC_FROMCLIENT = "rfc";
    public static final String RPC_REPLICATION = "replication";
    public static final String RPC_COMMIT = "commit";
    public static final String RPC_SYNC_TERM = "syncTerm";
    public static final String RPC_TOBESLAVE = "tobeslave";
    public static final String YES = "yes";
    public static final char FALSE = 'f';
    public static final char TRUE = 't';
    public static final String F_TRUE = "true";
    public static final String EXCEPTION = "E";
    public static final String CLUSTER_DOWN_MESSAGE = "cluster is down";
    public static final String ROOT_PATH;
    public static final String TIMEOUT = "timeout";
    public static final String ZERO = "0";
    public static final String ONE = "1";
    public static final String TWO = "2";

    static {
        String path = PropertyUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path.startsWith("file:")) {
            path = path.substring(5);
        }
        ROOT_PATH = path.substring(0, path.lastIndexOf("dracc-server"));
    }

}
