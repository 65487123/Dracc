
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

import java.util.List;

/**
 * Description:注册中心客户端接口
 *
 * @author: Zeping Lu
 * @date: 2021/4/30 17:53
 */
public class RegistryClient {
/*
    *//**
     * register a instance to service.
     *
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @throws Exception   exception
     *//*
    void registerInstance(String serviceName, String ip, int port) throws Exception;








    *//**
     * deregister instance from a service.
     *
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @throws Exception   exception
     *//*
    void deregisterInstance(String serviceName, String ip, int port) throws Exception;






    *//**
     * get all instances of a service.
     *
     * @param serviceName name of service
     * @return A list of instance
     * @throws Exception exception
     *//*
    List<Instance> getAllInstances(String serviceName) throws Exception;








    *//**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param listener    event listener
     * @throws Exception nacos exception
     *//*
    void subscribe(String serviceName, EventListener listener) throws NacosException;

    *//**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param listener    event listener
     * @throws NacosException nacos exception
     *//*
    void subscribe(String serviceName, String groupName, EventListener listener) throws NacosException;

    *//**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param clusters    list of cluster
     * @param listener    event listener
     * @throws NacosException nacos exception
     *//*
    void subscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException;

    *//**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param clusters    list of cluster
     * @param listener    event listener
     * @throws NacosException nacos exception
     *//*
    void subscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
            throws NacosException;

    *//**
     * Unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param listener    event listener
     * @throws NacosException nacos exception
     *//*
    void unsubscribe(String serviceName, EventListener listener) throws NacosException;

    *//**
     * unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param listener    event listener
     * @throws NacosException nacos exception
     *//*
    void unsubscribe(String serviceName, String groupName, EventListener listener) throws NacosException;

    *//**
     * Unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param clusters    list of cluster
     * @param listener    event listener
     * @throws NacosException nacos exception
     *//*
    void unsubscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException;

    *//**
     * Unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param clusters    list of cluster
     * @param listener    event listener
     * @throws NacosException nacos exception
     *//*
    void unsubscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
            throws NacosException;

    *//**
     * Get all service names from server.
     *
     * @param pageNo   page index
     * @param pageSize page size
     * @return list of service names
     * @throws NacosException nacos exception
     *//*
    ListView<String> getServicesOfServer(int pageNo, int pageSize) throws NacosException;

    *//**
     * Get all service names from server.
     *
     * @param pageNo    page index
     * @param pageSize  page size
     * @param groupName group name
     * @return list of service names
     * @throws NacosException nacos exception
     *//*
    ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName) throws NacosException;

    *//**
     * Get all service names from server with selector.
     *
     * @param pageNo   page index
     * @param pageSize page size
     * @param selector selector to filter the resource
     * @return list of service names
     * @throws NacosException nacos exception
     * @since 0.7.0
     *//*
    ListView<String> getServicesOfServer(int pageNo, int pageSize, AbstractSelector selector) throws NacosException;

    *//**
     * Get all service names from server with selector.
     *
     * @param pageNo    page index
     * @param pageSize  page size
     * @param groupName group name
     * @param selector  selector to filter the resource
     * @return list of service names
     * @throws NacosException nacos exception
     *//*
    ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName, AbstractSelector selector)
            throws NacosException;

    *//**
     * Get all subscribed services of current client.
     *
     * @return subscribed services
     * @throws NacosException nacos exception
     *//*
    List<ServiceInfo> getSubscribeServices() throws NacosException;*/

}
