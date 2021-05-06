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

package com.lzp.dracc.server.util;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.runtime.RuntimeSchema;

 /**
  * Description:序列化、反序列化对象的工具
  *
  * @author: Lu ZePing
  * @date: 2020/9/29 14:03
  */
 public class DataSearialUtil {

     private static RuntimeSchema<Data> schema = RuntimeSchema.createFrom(Data.class);

     /**
      * 序列化方法，将Data对象序列化为字节数组
      *
      * @param data
      * @return
      */
     public static byte[] serialize(Data data) {
         // Serializes the {@code message} into a byte array using the given schema
         return ProtostuffIOUtil.toByteArray(data, schema, LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));
     }

     /**
      * 反序列化方法，将字节数组反序列化为Data对象
      *
      * @param array
      * @return
      */
     public static Data deserialize(byte[] array) {
         Data data = schema.newMessage();
         // Merges the {@code message} with the byte array using the given {@code schema}
         ProtostuffIOUtil.mergeFrom(array, data, schema);
         return data;
     }
 }
