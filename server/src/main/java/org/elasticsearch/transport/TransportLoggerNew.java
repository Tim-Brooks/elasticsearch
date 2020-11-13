/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;

public final class TransportLoggerNew {

    private final Logger tracerLog;
    private volatile String[] tracerLogInclude;
    private volatile String[] tracerLogExclude;

    public TransportLoggerNew(Logger logger) {
        tracerLog = Loggers.getLogger(logger, ".tracer");
    }

    void traceRequestReceived(InboundMessage message) {
        String action = message.getHeader().getActionName();
        long requestId = message.getHeader().getRequestId();
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] received request", requestId, action);
        }
    }

    void traceRequestSent() {

    }

    void traceResponseReceived() {

    }

    void traceResponseSent() {

    }

    void traceErrorResponseSent() {

    }

    void traceUnresolvedResponseReceived() {

    }

    void traceTimedOutResponseReceived() {

    }

    private boolean shouldTraceAction(String action) {
        return shouldTraceAction(action, tracerLogInclude, tracerLogExclude);
    }

    public static boolean shouldTraceAction(String action, String[] include, String[] exclude) {
        if (include.length > 0) {
            if (Regex.simpleMatch(include, action) == false) {
                return false;
            }
        }
        if (exclude.length > 0) {
            return !Regex.simpleMatch(exclude, action);
        }
        return true;
    }
}
