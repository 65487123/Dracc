
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

package com.lzp.registry.javaclient.jregistry;

import com.lzp.registry.javaclient.EventListener;
import com.lzp.registry.javaclient.api.RegistryClient;
import com.lzp.registry.javaclient.exception.RegistryException;

import java.util.List;


public class JRegisty implements RegistryClient, AutoCloseable {

    public JRegisty(String... ipAndPorts) {

    }

    @Override
    public void registerInstance(String serviceName, String ip, int port) throws RegistryException {

    }

    @Override
    public void deregisterInstance(String serviceName, String ip, int port) throws RegistryException {

    }

    @Override
    public List<String> getAllInstances(String serviceName) throws RegistryException {
        return null;
    }

    @Override
    public void subscribe(String serviceName, EventListener listener) throws RegistryException {

    }

    @Override
    public void unsubscribe(String serviceName, EventListener listener) throws RegistryException {

    }

    @Override
    public void close() throws Exception {

    }
}
