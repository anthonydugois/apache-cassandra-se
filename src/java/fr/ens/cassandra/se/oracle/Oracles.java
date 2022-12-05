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

package fr.ens.cassandra.se.oracle;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a simple registry to avoid strong coupling between oracles and classes that make use of them. The main goal
 * is to allow the user to bind any oracle to a specific key; in this way, one may change the used oracle implementation
 * in scheduling algorithms directly from main configuration.
 */
public class Oracles
{
    public static final Oracles instance = new Oracles();

    private final Map<String, Oracle> oracles = new HashMap<>();

    public <K, V> void register(String key, Oracle<K, V> oracle)
    {
        oracles.put(key, oracle);
    }

    public <K, V> Oracle<K, V> get(String key)
    {
        return (Oracle<K, V>) oracles.get(key);
    }
}
