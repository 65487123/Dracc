
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
public interface DraccClient extends AutoCloseable{

    /**
     * 注册一个服务实例
     * 注意:
     * 1、注册的服务实例要和客户端本机ip一致。
     * 2、在关闭服务前不要close掉所有客户端。(至少要保证一个客户端存活,不然注册的实例会被标记为不健康并移除)
     *
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @return 操作是否改变了状态机
     * @throws DraccException exception
     */
    boolean registerInstance(String serviceName, String ip, int port) throws DraccException;


    /**
     * 注销一个服务实例
     * 注意：最好由谁注册的实例就由谁来注销。不然可能会出问题。
     *
     * @param serviceName 服务名 (唯一id)
     * @param ip          实例ip
     * @param port        实例端口
     * @return 操作是否改变了状态机
     * @throws DraccException exception
     */
    boolean deregisterInstance(String serviceName, String ip, int port) throws DraccException;


    /**
     * 获取服务的所有实例
     *
     * @param serviceName 服务名 (唯一id)
     * @return 实例list(以ip : port的形式)
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
     * @param serviceName 服务名 (唯一id)
     * @param listener    事件监听器
     * @throws DraccException exception
     */
    void unsubscribe(String serviceName, EventListener listener) throws DraccException;


    /**
     * 添加一个配置,原先不存在会添加
     *
     * @param configName 配置名(唯一id)
     * @param configVal  配置的值
     * @throws DraccException exception
     */
    void addConfig(String configName, String configVal) throws DraccException;


    /**
     * 移除一个配置
     *
     * @param configName 配置名(唯一id)
     * @param configVal  value of config
     * @throws DraccException exception
     */
    String removeConfig(String configName, String configVal) throws DraccException;


    /**
     * 根据配置名获取配置名的所有配置
     *
     * @param configName 配置名
     * @return 一个字符串list(所有配置值)
     * @throws DraccException exception
     */
    List<String> getConfig(String configName) throws DraccException;


    /**
     * 获取分布式锁
     * 同一个线程可以获取多次(可重入)
     * @param lockName 锁名(唯一id)
     * @throws DraccException exception
     */
    void acquireDistributedLock(String lockName) throws DraccException;


    /**
     * 释放分布式锁
     * 注意：虽然一个线程可以多次获取一把分布式锁,但是只要释放一次,这个线程就会释放这把锁。
     * <p>
     * server端没有对锁的获取次数进行计数(一个线程获取一次加一, 释放一次就减一)。
     * 因为这样可能会出现问题:当一个线程在获取锁时,成功拿到锁,server端计数器已经加一,然
     * 而在返回结果时网络断了,这样客户端如果重新执行获取锁操作,就会出现问题(获取一次锁,计数器加了多次)。
     * <p>
     * 总而言之,如果server端计数,获取锁和释放锁就不是幂等操作了。
     *
     * @param lockName 锁名(唯一id)
     * @return 当前锁被加锁次数。  如果释放的锁是根本不在加锁状态,返回-1
     * @throws DraccException exception
     */
    void releaseDistributedlock(String lockName) throws DraccException;


    /**
     * @return 如果本客户端已经关闭返回true,否则返回false
     */
    boolean isClosed();

}
