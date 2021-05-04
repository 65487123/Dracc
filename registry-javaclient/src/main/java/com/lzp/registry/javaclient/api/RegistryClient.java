
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

package com.lzp.registry.javaclient.api;

import com.lzp.registry.javaclient.EventListener;
import com.lzp.registry.javaclient.exception.RegistryException;

import java.util.List;

/**
 * Description:注册中心客户端接口
 *
 * @author: Zeping Lu
 * @date: 2021/4/30 17:53
 */
public interface RegistryClient {

    /**
     * register a instance to service.
     *
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @throws RegistryException   exception
     */
    void registerInstance(String serviceName, String ip, int port) throws RegistryException;



    /**
     * deregister instance from a service.
     *
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @throws RegistryException   exception
     */
    void deregisterInstance(String serviceName, String ip, int port) throws RegistryException;




    /**
     * get all instances of a service.
     *
     * @param serviceName name of service
     * @return A list of string(In the form of ip:port)
     * @throws RegistryException exception
     */
    List<String> getAllInstances(String serviceName) throws RegistryException;



    /**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param listener    event listener
     * @throws RegistryException nacos exception
     */
    void subscribe(String serviceName, EventListener listener) throws RegistryException;


    /**
     * Unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param listener    event listener
     * @throws RegistryException nacos exception
     */
    void unsubscribe(String serviceName, EventListener listener) throws RegistryException;



}
