/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright (C) 2006-2010 Adele Team/LIG/Grenoble University, France
 */
package fr.imag.adele.teamwork.db.test;

import java.io.Serializable;

public class Ser implements Serializable {
	
	private String m_id;

	public Ser(String id) {
		m_id = id;
	}
	
	public boolean equals(Object o) {
		if (o instanceof Ser) {
			return ((Ser) o).m_id.equals(this.m_id);
		} else
			return false;
	}
	
	public int hashCode() {
		return m_id.hashCode();
	}
}
