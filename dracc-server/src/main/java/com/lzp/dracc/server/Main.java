
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

package com.lzp.dracc.server;

import com.lzp.dracc.server.raft.RaftNode;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Description:后续可能会增加个前端界面,所以这里引入了springboot.(springboot内置servlet容器,启动后可以给前端提供接口)
 *
 * 现在springboot的作用就是把工程及其所依赖的库打成一个jar包,并且使得执行java -jar时候能进到这里(打包时会写meta-inf/MANIFEST.MF)
 * 其实通过在pom.xml引入下面插件也能实现
 * <plugin>
 *     <artifactId>maven-assembly-plugin</artifactId>
 *         <executions>
 *             <execution>
 *                 <phase>package</phase>
 *                 <goals>
 *                      <goal>single</goal>
 *                 </goals>
 *             </execution>
 *        </executions>
 *        <configuration>
 *            <descriptorRefs>
 *                <descriptorRef>jar-with-dependencies</descriptorRef>
 *            </descriptorRefs>
 *            <archive>
 *                <manifest>
 *                    <mainClass>com.utstar.msbtest.testsercer.Main</mainClass>
 *                </manifest>
 *            </archive>
 *       </configuration>
 * </plugin>
 *
 * @author: Zeping Lu
 * @date: 2021/2/15 13:17
 */

@SpringBootApplication
public class Main {

    public static void main(String[] args) {
        RaftNode.start();
    }
}

