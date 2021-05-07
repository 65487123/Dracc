
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

package com.lzp.dracc.javaclient.exception;

/**
 * Description:
 * 当server端节点个数少于整个集群节点个数的一半时,读写会抛这个异常
 *
 *
 * @author: Zeping Lu
 * @date: 2021/3/29 18:18
 */
public class TheClusterIsDownException extends RuntimeException {

    public TheClusterIsDownException(String message) {
        //"The number of surviving nodes is less than half of the total number of nodes in the raft cluster, and read operations are not supported"
        super(message);
    }

}
