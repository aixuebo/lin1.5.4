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

package org.apache.kylin.job.constant;

//因为一个kylin的job有若干个步骤,因此每一个步骤又相当于一个小job,也是有状态的,该状态就是本类的意义
public enum JobStepStatusEnum {
    NEW(0), //job的最开始的状态
    PENDING(1),//对应ExecutableState的状态是Ready,此时job是等待状态
    RUNNING(2),//对应的是ExecutableState的状态是RUNNING
    FINISHED(4),//对应的是ExecutableState的状态是SUCCESS,说明job完成了
    ERROR(8),//对应的是ExecutableState的状态是ERROR
    DISCARDED(16),//丢弃  对应的是ExecutableState的状态是DISCARDED

    WAITING(32),
    KILLED(64);

    private final int code;

    private JobStepStatusEnum(int statusCode) {
        this.code = statusCode;
    }

    public static JobStepStatusEnum getByCode(int statusCode) {
        for (JobStepStatusEnum status : values()) {
            if (status.getCode() == statusCode) {
                return status;
            }
        }

        return null;
    }

    public int getCode() {
        return this.code;
    }

    public boolean isComplete() {
        return code == JobStepStatusEnum.FINISHED.getCode() || code == JobStepStatusEnum.ERROR.getCode() || code == JobStepStatusEnum.DISCARDED.getCode();
    }

    public boolean isRunable() {
        return code == JobStepStatusEnum.PENDING.getCode() || code == JobStepStatusEnum.ERROR.getCode();
    }
}
