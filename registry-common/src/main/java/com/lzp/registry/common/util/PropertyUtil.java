
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

package com.lzp.registry.common.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Description:读取配置文件工具类
 *
 * @author: Zeping Lu
 * @date: 2021/3/19 15:34
 */
public class PropertyUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyUtil.class);

    public static Properties getProperties(String fileName) {
        Properties properties = new Properties();
        InputStream in = PropertyUtil.class.getClassLoader().getResourceAsStream("config/" + fileName);
        try {
            properties.load(in);
        } catch (Exception e) {
            LOGGER.error("load zprpc.properties failed", e);
        }
        return properties;
    }
}
