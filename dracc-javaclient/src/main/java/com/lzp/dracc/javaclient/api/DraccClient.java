
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
     * register a instance to service.
     *
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @throws DraccException   exception
     */
    void registerInstance(String serviceName, String ip, int port) throws DraccException;



    /**
     * deregister instance from a service.
     *
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @throws DraccException   exception
     */
    void deregisterInstance(String serviceName, String ip, int port) throws DraccException;



    /**
     * get all instances of a service.
     *
     * @param serviceName name of service
     * @return A list of string(In the form of ip:port)
     * @throws DraccException exception
     */
    List<String> getAllInstances(String serviceName) throws DraccException;



    /**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param listener    event listener
     * @throws DraccException exception
     */
    void subscribe(String serviceName, EventListener listener) throws DraccException;



    /**
     * Unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param listener    event listener
     * @throws DraccException exception
     */
    void unsubscribe(String serviceName, EventListener listener) throws DraccException;



    /**
     * add a config.
     *
     * @param configName name of config
     * @param configVal  value of config
     * @throws DraccException   exception
     */
    void addConfig(String configName, String configVal) throws DraccException;



    /**
     * remove a config.
     *
     * @param configName name of config
     * @param configVal  value of config
     * @throws DraccException   exception
     */
    void removeConfig(String configName, String configVal) throws DraccException;



    /**
     * get all instances of a service.
     *
     * @param configName name of config
     * @return A list of string(all configs)
     * @throws DraccException exception
     */
    List<String> getConfigs(String configName) throws DraccException;



    /**
     * Obtain a distributed lock.
     *
     * @param lockName name of lock
     * @throws DraccException exception
     */
    void lock(String lockName) throws DraccException;



    /**
     * Release the distributed lock.
     *
     * @param lockName name of lock
     * @throws DraccException exception
     */
    void unlock(String lockName) throws DraccException;




}
