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

package fr.ens.cassandra.se;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.cassandra.locator.InetAddressAndPort;

public class ClusterState
{
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ConcurrentMap<InetAddressAndPort, EndpointState> endpoints = new ConcurrentHashMap<>();

    public EndpointState endpoint(InetAddressAndPort address)
    {
        EndpointState endpoint = endpoints.get(address);

        if (endpoint == null)
        {
            endpoint = new EndpointState();

            EndpointState currentEndpoint = endpoints.putIfAbsent(address, endpoint);

            if (currentEndpoint == null)
            {
                executor.execute(endpoint);
            }
            else
            {
                endpoint = currentEndpoint;
            }
        }

        return endpoint;
    }

    public void push(StateEntries feedback)
    {
        endpoint(feedback.getAddress()).push(feedback);
    }

    public static class EndpointState implements Runnable
    {
        private final BlockingQueue<StateEntries> queue = new PriorityBlockingQueue<>();

        public void push(StateEntries feedback)
        {
            try
            {
                queue.put(feedback);
            }
            catch (InterruptedException exception)
            {
                throw new RuntimeException(exception);
            }
        }

        @Override
        public void run()
        {
            try
            {
                while (true)
                {
                    StateEntries feedback = queue.take();

                    // TODO: retrieve all values in StateFeedback, and apply aggregation procedure on each value
                }
            }
            catch (InterruptedException exception)
            {
                throw new RuntimeException(exception);
            }
        }
    }
}
