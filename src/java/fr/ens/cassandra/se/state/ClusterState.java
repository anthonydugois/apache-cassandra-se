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

package fr.ens.cassandra.se.state;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import fr.ens.cassandra.se.state.facts.Fact;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

public class ClusterState
{
    public static final ClusterState instance = new ClusterState();

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final ConcurrentMap<InetAddressAndPort, EndpointState> states = new ConcurrentHashMap<>();

    public EndpointState state(InetAddressAndPort address)
    {
        EndpointState state = states.get(address);

        if (state == null)
        {
            state = new EndpointState(address);

            EndpointState currentState = states.putIfAbsent(address, state);

            if (currentState == null)
            {
                executor.execute(state);
            }
            else
            {
                state = currentState;
            }
        }

        return state;
    }

    public void updateLocal()
    {
        InetAddressAndPort address = FBUtilities.getLocalAddressAndPort();
        StateFeedback feedback = new StateFeedback();

        feedback.put(Fact.facts());

        state(address).add(feedback);
    }
}
