/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.module.extension.internal.runtime.operation;

import static java.util.Optional.ofNullable;
import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static org.mule.runtime.api.util.Preconditions.checkArgument;
import static org.mule.runtime.core.api.lifecycle.LifecycleUtils.initialiseIfNeeded;
import static org.mule.runtime.core.privileged.processor.MessageProcessors.processWithChildContext;
import static reactor.core.publisher.Mono.from;
import org.mule.runtime.api.exception.MuleRuntimeException;
import org.mule.runtime.api.lifecycle.Initialisable;
import org.mule.runtime.api.lifecycle.InitialisationException;
import org.mule.runtime.api.message.Message;
import org.mule.runtime.api.metadata.MediaType;
import org.mule.runtime.api.metadata.TypedValue;
import org.mule.runtime.core.api.MuleContext;
import org.mule.runtime.core.api.event.BaseEvent;
import org.mule.runtime.core.api.event.BaseEventContext;
import org.mule.runtime.core.api.exception.MessagingException;
import org.mule.runtime.core.api.processor.Processor;
import org.mule.runtime.core.internal.message.InternalEvent;
import org.mule.runtime.core.privileged.processor.chain.ChainExecutorContext;
import org.mule.runtime.core.privileged.processor.chain.MessageProcessorChain;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.extension.api.runtime.route.Chain;
import org.mule.runtime.module.extension.api.runtime.privileged.EventedResult;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.inject.Inject;

/**
 * An implementation of {@link Chain} that wraps a {@link Processor} and allows to execute it
 *
 * @since 4.0
 */
public class ImmutableProcessorChainExecutor implements Chain, Initialisable {

  /**
   * Event that will be cloned for dispatching
   */
  private final BaseEvent originalEvent;

  /**
   * Processor that will be executed upon calling process
   */
  private final MessageProcessorChain chain;

  @Inject
  private MuleContext muleContext;

  private BaseEvent currentEvent;

  private Consumer<Result> successHandler;
  private BiConsumer<Throwable, Result> errorHandler;

  private Function<Result, Result> eachSuccessHandler;
  private ErrorHandler eachErrorHandler;

  /**
   * Creates a new immutable instance
   *
   * @param event the original {@link BaseEvent} for the execution of the given chain
   * @param chain a {@link Processor} chain to be executed
   */
  public ImmutableProcessorChainExecutor(BaseEvent event, MessageProcessorChain chain) {
    this.originalEvent = event;
    this.currentEvent = event;
    this.chain = chain;
  }

  @Override
  public void process(Consumer<Result> onSuccess, BiConsumer<Throwable, Result> onError) {
    doProcess(originalEvent, onSuccess, onError);
  }

  @Override
  public void process(Object payload, Object attributes, Consumer<Result> onSuccess, BiConsumer<Throwable, Result> onError) {
    process(Result.builder().output(payload).attributes(attributes).build(),
            onSuccess, onError);
  }

  @Override
  public void process(Result result, Consumer<Result> onSuccess, BiConsumer<Throwable, Result> onError) {
    currentEvent = result instanceof EventedResult
      ? ((EventedResult) result).getEvent()
      : copyAndUpdate(currentEvent, result);

    doProcess(currentEvent, onSuccess, onError);
  }

  @Override
  public Chain onEachSuccess(Function<Result, Result> interceptor) {
    this.eachSuccessHandler = interceptor;
    return this;
  }

  @Override
  public Chain onEachError(ErrorHandler interceptor) {
    this.eachErrorHandler = interceptor;
    return this;
  }

  private void setCompletionHandlers(Consumer<Result> onSuccess, BiConsumer<Throwable, Result> onError) {
    checkArgument(onSuccess != null,
                  "A success completion handler is required in order to execute the components chain, but it was null");
    checkArgument(onError != null,
                  "An error completion handler is required in order to execute the components chain, but it was null");

    this.successHandler = onSuccess;
    this.errorHandler = onError;
  }

  private void doProcess(BaseEvent updatedEvent, Consumer<Result> onSuccess, BiConsumer<Throwable, Result> onError) {
    if (isEmpty(chain.getMessageProcessors())) {
      onSuccess.accept(EventedResult.from(updatedEvent));
      return;
    }

    setCompletionHandlers(onSuccess, onError);
    InternalEvent.Builder builder = InternalEvent.builder(updatedEvent);
    if (eachSuccessHandler != null || eachErrorHandler != null) {
      builder.addInternalParameter("ChainExecutorContext",
                                   new ChainExecutorContext(eachSuccessHandler != null ? this::handleEachSuccess : null,
                                                            eachErrorHandler != null ? this::handleEachError : null));
    }

    currentEvent = builder.build();
    from(processWithChildContext(currentEvent, chain, ofNullable(chain.getLocation())))
      .map(event -> InternalEvent.builder(event)
        .removeInternalParameter("ChainExecutorContext")
        .build())
      .doOnSuccess(this::handleChainSuccess)
      .doOnError(MessagingException.class, error -> this.handleChainError(error, error.getEvent()))
      .doOnError(error -> this.handleChainError(error, currentEvent))
      .subscribe();
  }

  private BaseEvent handleEachSuccess(BaseEvent previousResult) {
    currentEvent = previousResult != null ? previousResult : copyAndUpdate(currentEvent, Result.builder().build());

    Result input = EventedResult.from(currentEvent);
    Result output;
    try {
      output = eachSuccessHandler.apply(input);
    } catch (RuntimeException error) {
      if (eachErrorHandler == null) {
        throw error;
      }
      try {
        output = eachErrorHandler.apply(error, input);
      } catch (Throwable throwable) {
        throw new MuleRuntimeException(throwable);
      }
    }

    currentEvent = output instanceof EventedResult
      ? ((EventedResult) output).getEvent()
      : copyAndUpdate(currentEvent, output);

    return currentEvent;
  }

  private BaseEvent handleEachError(Throwable error, BaseEvent childEvent) throws Throwable {
    currentEvent = childEvent != null ? childEvent : copyAndUpdate(currentEvent, Result.builder().build());

    Result output = eachErrorHandler.apply(error, EventedResult.from(currentEvent));
    currentEvent = output instanceof EventedResult
      ? ((EventedResult) output).getEvent()
      : copyAndUpdate(currentEvent, output);

    return currentEvent;
  }

  private void handleChainSuccess(BaseEvent childEvent) {
    currentEvent = childEvent != null ? childEvent : copyAndUpdate(currentEvent, Result.builder().build());
    EventedResult result = EventedResult.from(currentEvent);

    try {
      successHandler.accept(result);
    } catch (Throwable error) {
      errorHandler.accept(error, result);
    }
  }

  private BaseEvent handleChainError(Throwable error, BaseEvent childEvent) {
    try {
      currentEvent = childEvent != null ? childEvent : copyAndUpdate(currentEvent, Result.builder().build());
      errorHandler.accept(error, EventedResult.from(currentEvent));
    } catch (Throwable e) {
      ((BaseEventContext) originalEvent.getContext()).error(e);
    }
    return null;
  }

  private BaseEvent copyAndUpdate(BaseEvent base, Result result) {
    Message.Builder builder = Message.builder().payload(TypedValue.of(result.getOutput()));
    result.getAttributes().ifPresent(attributes -> builder.attributes(TypedValue.of(attributes)));
    result.getMediaType().ifPresent(mediatype -> builder.mediaType((MediaType) mediatype));
    result.getAttributesMediaType().ifPresent(mediatype -> builder.attributesMediaType((MediaType) mediatype));

    return BaseEvent.builder(base).message(builder.build()).build();
  }

  @Override
  public void initialise() throws InitialisationException {
    initialiseIfNeeded(chain);
  }

}
