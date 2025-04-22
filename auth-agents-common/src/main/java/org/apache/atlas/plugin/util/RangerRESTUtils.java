/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.plugin.util;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Since this class does not retain any state.  It isn't a singleton for testability.
 *
 */
public class RangerRESTUtils {

	private static final Log LOG = LogFactory.getLog(RangerRESTUtils.class);

	private static final int MAX_PLUGIN_ID_LEN = 255;


	public static String hostname;

	static {
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		}
		catch(Exception e) {
			LOG.error("ERROR: Unable to find hostname for the agent ", e);
			hostname = "unknownHost";
		}
	}

    public String getPluginId(String serviceName, String appId) {
        String hostName = null;

        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("ERROR: Unable to find hostname for the agent ", e);
            hostName = "unknownHost";
        }

        String ret  = hostName + "-" + serviceName;

        if(! StringUtils.isEmpty(appId)) {
        	ret = appId + "@" + ret;
        }

        if (ret.length() > MAX_PLUGIN_ID_LEN ) {
        	ret = ret.substring(0,MAX_PLUGIN_ID_LEN);
        }

        return ret ;
    }
    /*
     * This method returns the hostname of agents.
     */
    public String getAgentHostname() {
        return hostname;
    }
}
