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

import java.util.Map;

public class ClassKeySizeOracle extends AbstractOracle<String, ClassKeySizeOracle.SizeClass>
{
    public enum SizeClass
    {
        SMALL,
        LARGE
    }

    public static final String THRESHOLD_PROPERTY = "threshold";
    public static final String DEFAULT_THRESHOLD_PROPERTY = "1024";

    private final long threshold;

    private final KeySizeOracle oracle;

    public ClassKeySizeOracle(Map<String, String> parameters)
    {
        super(parameters);

        this.threshold = Long.parseLong(parameters.getOrDefault(THRESHOLD_PROPERTY, DEFAULT_THRESHOLD_PROPERTY));

        this.oracle = new KeySizeOracle(parameters);
    }

    @Override
    public void init()
    {
        oracle.init();
    }

    @Override
    public SizeClass get(String key)
    {
        int size = oracle.get(key);

        if (size > threshold)
        {
            return SizeClass.LARGE;
        }

        return SizeClass.SMALL;
    }
}
