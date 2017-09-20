/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.privileged.processor.chain;

import org.mule.runtime.core.api.event.BaseEvent;

import java.util.Optional;

public final class ChainExecutorContext {

  private final SuccessHandler eachSuccessHandler;
  private final ErrorHandler eachErrorHandler;

  public ChainExecutorContext(SuccessHandler eachSuccessHandler,
                              ErrorHandler eachErrorHandler) {
    this.eachSuccessHandler = eachSuccessHandler;
    this.eachErrorHandler = eachErrorHandler;
  }

  public Optional<SuccessHandler> getEachSuccessHandler() {
    return Optional.ofNullable(eachSuccessHandler);
  }

  public Optional<ErrorHandler> getEachErrorHandler() {
    return Optional.ofNullable(eachErrorHandler);
  }

  @FunctionalInterface
  public interface SuccessHandler {

    BaseEvent apply(BaseEvent previousResult);

  }


  @FunctionalInterface
  public interface ErrorHandler {

    BaseEvent apply(Throwable error, BaseEvent previousResult) throws Throwable;
  }
}
