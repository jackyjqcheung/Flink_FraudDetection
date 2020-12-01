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

package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.io.IOException;

/**
 * 欺诈交易检测的业务逻辑,实现有状态流处理程序
 * 欺诈检查类 FraudDetector 是 KeyedProcessFunction 接口的一个实现。
 * 他的方法 KeyedProcessFunction#processElement 将会在每个交易事件上被调用。这个程序里边会对每笔交易发出警报。
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timer-state", Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        Boolean lastTransactionWasSmall = flagState.value();
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            // 清理我们定义的状态
            cleanUp(context);
        }
        if (transaction.getAmount() < SMALL_AMOUNT) {
            // 如小于1则设置true，创建定时器 1min以后 触发
            flagState.update(true);
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            // 注意这里的定时器是基于processTime
            context.timerService().registerEventTimeTimer(timer);
            timerState.update(timer);
        }


    }

    /**
     * 定时器触发后执行的方法
     *
     * @param timestamp 该定时器的触发时间
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        Long currentKey = ctx.getCurrentKey();
        Long value = timerState.value();
        System.out.println(String.format("currentKey:%d,value:%d", currentKey, value));

        // 1min后清理状态
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context context) throws Exception {
        Long timer = timerState.value();
        context.timerService().deleteProcessingTimeTimer(timer);

        // 清理所有的状态
        timerState.clear();
        flagState.clear();
    }
}
