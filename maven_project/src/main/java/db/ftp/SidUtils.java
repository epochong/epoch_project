/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.genitus.shaft.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sid convertors.
 * <p>Sid format: ath********@bj***********####000</p>
 *
 * @author gwjiang (gwjiang@iflytek.com), 2017/12/7.
 */
public class SidUtils {
  /** length of sid. */
  private static final int SID_SIZE = 32;

  /** sid regex. */
  private static final String SID_REGEX = "^[a-zA-Z]{3}[0-9a-zA-Z]{8}@[a-zA-Z]{2}[0-9a-zA-Z]{11}[0-9a-fA-F]{4}[0-9a-zA-Z]{3}$";

  /** sid pattern. */
  private static final Pattern SID_PATTERN = Pattern.compile(SID_REGEX);

  /**
   * Whether the specific sid is valide.
   *
   * @param sid sid.
   * @return {@code true} if sid is valide.
   */
  public static boolean valid(String sid) {
    if (sid == null || sid.length() != SID_SIZE) {
      return false;
    }

    Matcher matcher = SID_PATTERN.matcher(sid);
    return matcher.find();
  }

  /**
   * Get timestampe in mills of specific sid.
   *
   * @param sid sid.
   * @return timestampe in mills.
   */
  public static long toTimestamp(String sid) {
    if (null == sid || sid.length() != SID_SIZE) {
      return -1;
    }

    return Long.parseLong(sid.substring(14, 25), 16);
  }

  /**
   * Get timestampe in mills of specific sid.
   *
   * @param sid sid.
   * @return timestampe in mills.
   */
  public static long toTimestampMsp(String sid) {
    if (null == sid || sid.length() != SID_SIZE) {
      return -1;
    }

    return 1000L * Long.parseLong(sid.substring(18, 26), 16) + 1285862400000L;
  }
}
