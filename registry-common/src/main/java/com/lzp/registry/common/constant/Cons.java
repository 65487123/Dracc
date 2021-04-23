
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

package com.lzp.registry.common.constant;

import com.lzp.registry.common.util.PropertyUtil;

/**
 * Description:常量类
 *
 * @author: Zeping Lu
 * @date: 2021/3/17 16:26
 */
public class Cons {
    public static final String COMMA = ",";
    public static final String COLON = ":";
    public static final String COPY_LOG_REQ = "cp_req";
    public static final String COPY_LOG_REPLY = "cp_reply";
    public static final String COMMAND_SEPARATOR = "-:";
    public static final String SPECIFICORDER_SEPARATOR = "-=";
    public static final String CLU_PRO = "cluster.properties";
    public static final String PERSI_PRO = "persistence.properties";
    public static final String SNAPSHOT_BATCH_COUNT = "snapshot-batch-count";
    public static final String RPC_ASKFORVOTE = "reqforvote";
    public static final String RPC_FROMCLIENT = "fromcli";
    public static final String RPC_REPLICATION = "replication";
    public static final String RPC_COMMIT = "commit";
    public static final String RPC_SYNC_TERM = "syncTerm";
    public static final String RPC_TOBESLAVE = "tobeslave";
    public static final String GET = "get";
    public static final String ADD = "add";
    public static final String REM = "rem";
    public static final String YES = "yes";
    public static final String FALSE = "false";
    public static final String TRUE = "true";
    public static final String EXCEPTION = "E";
    public static final String CLUSTER_DOWN_MESSAGE = "Less than half of the nodes survive, and the cluster does not provide services";
    public static final String ROOT_PATH;

    static {
        String path = PropertyUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (System.getProperty("os.name").contains("dows")) {
            path = path.substring(5);
        }
        ROOT_PATH = path.substring(0, path.lastIndexOf("registry-server"));
    }

}
