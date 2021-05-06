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

/**
 * Description:回调接口
 *
 * @author: Zeping Lu
 * @date: 2020/11/13 12:48
 */
public interface FutureCallback<R> {
    /**
     * @return
     * @description 当future已经成功完成，执行的回调方法
     * @param r future返回的结果
     */
    void onSuccess(R r);

    /**
     * @return
     * @description 当future失败时，执行的回调方法
     * @param t future抛出的异常或错误
     */
    void onFailure(Throwable t);
}
