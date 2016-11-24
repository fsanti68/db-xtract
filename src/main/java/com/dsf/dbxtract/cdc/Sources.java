/**
 * Copyright 2016 Fabio De Santi
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dsf.dbxtract.cdc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * 
 * @author fabio de santi
 */
@XmlRootElement
public class Sources {

	private List<Source> sourceList = null;
	private long interval = 5000L;

	public List<Source> getSources() {
		if (sourceList == null)
			sourceList = new ArrayList<Source>();
		return sourceList;
	}

	/**
	 * 
	 * @return milliseconds between capture cycles
	 */
	public long getInterval() {
		return interval;
	}

	public void setInterval(long millisecs) {
		this.interval = millisecs;
	}

	@Override
	public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append("Sources {interval=").append(interval).append(", sources=[");
		Iterator<Source> it = getSources().iterator();
		while (it.hasNext()) {
			Source src = it.next();
			sb.append(src.toString());
			if (it.hasNext())
				sb.append(", ");
		}
		sb.append("]}");
		return sb.toString();
	}
}
