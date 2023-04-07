/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.audit.es.res;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.cassandra.audit.es.common.ErrorEnum;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataRsp<T> {

    @Builder.Default
    private int code=0;

    @Builder.Default
    private String message="SUCCESS";

    private T data;

    public DataRsp(int code, String message){
        this.code = code;
        this.message = message;
    }

    public DataRsp(ErrorEnum error){
        this.code = error.code;
        this.message=error.message;
    }

    public static DataRsp getError406(){
        return DataRsp.builder()
                .code(ErrorEnum.ES_URL_NOT_FOND.code)
                .message(ErrorEnum.ES_URL_NOT_FOND.message)
                .build();
    }


    public static DataRsp getError200(){
        return DataRsp.builder()
                .code(ErrorEnum.SUCCESS.code)
                .message(ErrorEnum.SUCCESS.message)
                .build();
    }
}
