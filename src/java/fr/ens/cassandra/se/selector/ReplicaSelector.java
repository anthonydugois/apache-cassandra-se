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

package fr.ens.cassandra.se.selector;

import java.util.Map;
import java.util.Set;

import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaCollection;

public abstract class ReplicaSelector extends AbstractEndpointSnitch
{
    protected final IEndpointSnitch snitch;

    protected final Map<String, String> parameters;

    protected ReplicaSelector(IEndpointSnitch snitch, Map<String, String> parameters)
    {
        this.snitch = snitch;
        this.parameters = parameters;
    }

    public IEndpointSnitch getSnitch()
    {
        return snitch;
    }

    public Map<String, String> getParameters()
    {
        return parameters;
    }

    @Override
    public String getRack(InetAddressAndPort endpoint)
    {
        return snitch.getRack(endpoint);
    }

    @Override
    public String getDatacenter(InetAddressAndPort endpoint)
    {
        return snitch.getDatacenter(endpoint);
    }

    @Override
    public void gossiperStarting()
    {
        snitch.gossiperStarting();
    }

    @Override
    public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
    {
        return snitch.isWorthMergingForRangeQuery(merged, l1, l2);
    }

    @Override
    public boolean validate(Set<String> datacenters, Set<String> racks)
    {
        return snitch.validate(datacenters, racks);
    }
}
