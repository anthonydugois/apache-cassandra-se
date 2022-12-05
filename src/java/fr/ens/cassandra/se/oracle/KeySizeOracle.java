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

/**
 * This oracle infers key size from a partition key that has the following form: {size}_{id}, where {size} corresponds
 * to the number of bytes of the associated value, and {id} is a unique identifier.
 *
 * This oracle has two advantages over CSVKeySizeOracle:
 * - no additional memory is needed;
 * - write-after-rampup operations are automatically handled (no need to update the key-size memory map).
 *
 * Unless there is a need to perform specific calculations over the full set of data (for example, computing some
 * statistics), this oracle should be prefered over CSVKeySizeOracle.
 */
public class KeySizeOracle extends AbstractOracle<String, Integer>
{
    public KeySizeOracle(Map<String, String> parameters)
    {
        super(parameters);
    }

    @Override
    public Integer get(String key)
    {
        String[] values = key.split("_", 2);

        return Integer.parseInt(values[0]);
    }
}
