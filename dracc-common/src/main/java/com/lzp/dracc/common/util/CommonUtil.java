
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

package com.lzp.dracc.common.util;

import com.lzp.dracc.common.constant.Const;

import java.util.*;

/**
 * Description:一些通用的工具方法
 *
 * @author: Zeping Lu
 * @date: 2021/3/29 17:11
 */
public class CommonUtil {


    /**
     * 把Set<String>类型序列化成字符串,server端返回查询结果用
     */
    public static String serial(Set<String> set) {
        if (set == null) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder(128);
        for (String element : set) {
            stringBuilder.append(element).append(Const.COMMAND_SEPARATOR);
        }
        return stringBuilder.deleteCharAt(stringBuilder.length() - 1).toString();
    }

    /**
     * 字符串序列化成list,java客户端获取查询结果用
     */
    public static List<String> deserial(String elements) {
        return new ArrayList<>(Arrays.asList(elements.split(Const.COMMAND_SEPARATOR)));
    }

}
