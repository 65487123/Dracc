
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

package com.lzp.dracc.javaclient.api;

import com.lzp.dracc.javaclient.EventListener;
import com.lzp.dracc.javaclient.exception.DraccException;

import java.util.List;

/**
 * Description:注册中心客户端接口
 *
 * @author: Zeping Lu
 * @date: 2021/4/30 17:53
 */
public interface DraccClient {

    /**
     * 注册一个服务实例
     *
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @throws DraccException   exception
     */
    void registerInstance(String serviceName, String ip, int port) throws DraccException;



    /**
     * 注销一个服务实例
     * 注意：由谁注册的实例就由谁来注销。不然可能会出问题
     *
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @throws DraccException   exception
     */
    void deregisterInstance(String serviceName, String ip, int port) throws DraccException;



    /**
     * 获取服务的所有实例
     *
     * @param serviceName name of service
     * @return A list of string(In the form of ip:port)
     * @throws DraccException exception
     */
    List<String> getAllInstances(String serviceName) throws DraccException;



    /**
     * 订阅服务以接收实例更改的事件
     * 注意：
     * 1、监听器不会收到由本客户端修改而产生的事件,这样做是为了节省资源(本客户端对server端做的修改,
     * 自己是知道的,server端没必要再向这个客户端发一次通知)
     * 2、注册的监听器不会因为本客户端被关闭(close)而被清除,需要主动unsubscribe()
     *
     * @param serviceName 服务名
     * @param listener    事件监听器
     * @throws DraccException 异常
     */
    void subscribe(String serviceName, EventListener listener) throws DraccException;



    /**
     * 取消对某个服务的订阅
     *
     * @param serviceName name of service
     * @param listener    event listener
     * @throws DraccException exception
     */
    void unsubscribe(String serviceName, EventListener listener) throws DraccException;



    /**
     * 添加一个配置
     * 注意:添加的配置名不要和被监听的服务名一样
     *
     * @param configName name of config
     * @param configVal  value of config
     * @throws DraccException   exception
     */
    void addConfig(String configName, String configVal) throws DraccException;



    /**
     * 移除一个配置
     *
     * @param configName name of config
     * @param configVal  value of config
     * @throws DraccException   exception
     */
    void removeConfig(String configName, String configVal) throws DraccException;



    /**
     * 根据配置名获取配置名的所有配置
     *
     * @param configName name of config
     * @return A list of string(all configs)
     * @throws DraccException exception
     */
    List<String> getConfigs(String configName) throws DraccException;



    /**
     * Acquire a distributed lock.
     *
     * @param lockName name of lock
     * @throws DraccException exception
     */
    void acquireDistributedLock(String lockName) throws DraccException;



    /**
     * Release the distributed lock.
     *
     * @param lockName name of lock
     * @throws DraccException exception
     */
    void releaseDistributedlock(String lockName) throws DraccException;


}
