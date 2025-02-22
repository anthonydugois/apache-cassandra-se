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

package fr.ens.cassandra.se.state.facts.impl;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import fr.ens.cassandra.se.state.StateFeedback;
import fr.ens.cassandra.se.state.facts.FactAggregator;

public class PendingReadAggregator implements FactAggregator<ExponentiallyDecayingReservoir, Integer>
{
    private static final double ALPHA = 0.75;
    private static final int WINDOW_SIZE = 100;

    @Override
    public ExponentiallyDecayingReservoir get()
    {
        return new ExponentiallyDecayingReservoir(WINDOW_SIZE, ALPHA);
    }

    @Override
    public ExponentiallyDecayingReservoir apply(ExponentiallyDecayingReservoir current, Integer value, StateFeedback feedback)
    {
        current.update(value);
        return current;
    }
}
