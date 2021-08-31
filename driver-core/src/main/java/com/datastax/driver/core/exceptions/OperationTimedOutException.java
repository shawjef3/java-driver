/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.SocketOptions;

/**
 * Thrown on a client-side timeout, i.e. when the client didn't hear back from the server within
 * {@link SocketOptions#getReadTimeoutMillis()}.
 */
public class OperationTimedOutException extends ConnectionException {

  private static final long serialVersionUID = 0;

  public OperationTimedOutException(EndPoint endPoint) {
    super(endPoint, "Operation timed out");
  }

  public OperationTimedOutException(EndPoint endPoint, String msg) {
    super(endPoint, msg);
  }

  public OperationTimedOutException(EndPoint endPoint, String msg, Throwable cause) {
    super(endPoint, msg, cause);
  }

  @Override
  public OperationTimedOutException copy() {
    return new OperationTimedOutException(getEndPoint(), getRawMessage(), this);
  }
}
