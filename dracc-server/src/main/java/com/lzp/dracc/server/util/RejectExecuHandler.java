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

package com.lzp.dracc.server.util;

import java.util.concurrent.RejectedExecutionException;

 /**
  * Description:拒接策略接口
  *
  * @author: Lu ZePing
  * @date: 2019/7/20 12:19
  */
 public interface RejectExecuHandler {

     /**
      * Method that may be invoked by a {@link ThreadPoolExecutor} when
      * {@link java.util.concurrent.ThreadPoolExecutor#execute execute} cannot accept a
      * task.  This may occur when no more threads or queue slots are
      * available because their bounds would be exceeded, or upon
      * shutdown of the Executor.
      *
      * <p>In the absence of other alternatives, the method may throw
      * an unchecked {@link RejectedExecutionException}, which will be
      * propagated to the caller of {@code execute}.
      *
      * @param r the runnable task requested to be executed
      * @param executor the executor attempting to execute this task
      * @throws RejectedExecutionException if there is no remedy
      */
     void rejectedExecution(Runnable r, ThreadPoolExecutor executor);

 }
