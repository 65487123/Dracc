
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

package com.lzp.dracc.javaclient.jdracc;

import com.lzp.dracc.javaclient.EventListener;
import com.lzp.dracc.javaclient.api.DraccClient;
import com.lzp.dracc.javaclient.exception.DraccException;

import java.util.List;


public class JRegisty implements DraccClient, AutoCloseable {

    public JRegisty(String... ipAndPorts) {

    }

    @Override
    public void registerInstance(String serviceName, String ip, int port) throws DraccException {

    }

    @Override
    public void deregisterInstance(String serviceName, String ip, int port) throws DraccException {

    }

    @Override
    public List<String> getAllInstances(String serviceName) throws DraccException {
        return null;
    }

    @Override
    public void subscribe(String serviceName, EventListener listener) throws DraccException {

    }

    @Override
    public void unsubscribe(String serviceName, EventListener listener) throws DraccException {

    }

    @Override
    public void addConfig(String configName, String configVal) throws DraccException {

    }

    @Override
    public void removeConfig(String configName, String configVal) throws DraccException {

    }

    @Override
    public List<String> getConfigs(String configName) throws DraccException {
        return null;
    }

    @Override
    public void acquireDistributedLock(String lockName) throws DraccException {

    }

    @Override
    public void releaseDistributedlock(String lockName) throws DraccException {

    }


    @Override
    public void close() throws Exception {

    }
}
