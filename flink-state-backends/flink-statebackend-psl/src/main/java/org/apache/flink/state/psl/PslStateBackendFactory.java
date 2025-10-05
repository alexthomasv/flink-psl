/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.psl;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;

import java.io.IOException;

/**
 * Factory that creates a {@link PslStateBackend} from a {@link ReadableConfig}.
 *
 * <p>This class is discovered via reflection when {@code state.backend} is configured to this
 * factory’s FQN. It is responsible for wiring optional flags from the configuration and returning a
 * fully constructed backend instance.
 */
public final class PslStateBackendFactory implements StateBackendFactory<StateBackend> {

    /**
     * Creates the PSL state backend using the provided configuration.
     *
     * @param config the Flink configuration view available to the job / task
     * @param classLoader the user-code class loader Flink provides
     * @return a new {@link PslStateBackend}
     * @throws IllegalConfigurationException if configuration is invalid
     * @throws IOException if backend initialization needs to report IO failures
     */
    @Override
    public StateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException, IOException {

        // Example: wire options from config if you add them later
        // boolean linear = config.get(PslStateBackendOptions.LINEARIZABLE_READS);
        boolean linear = false;

        // TODO: build a real KVSClient from your net/rpc configs.
        String kvsConfig = "";

        return new PslStateBackend(linear, kvsConfig);
    }
}
