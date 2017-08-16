/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.routing;

import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static org.mule.runtime.api.message.Message.builder;
import static org.mule.runtime.core.api.processor.MessageProcessors.newChain;
import static org.mule.runtime.core.api.routing.ForkJoinStrategy.RoutingPair.of;
import static org.mule.runtime.core.internal.routing.ExpressionSplittingStrategy.DEFAULT_SPIT_EXPRESSION;
import static reactor.core.publisher.Flux.fromIterable;
import static reactor.core.publisher.Flux.just;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.lifecycle.InitialisationException;
import org.mule.runtime.api.message.Message;
import org.mule.runtime.api.metadata.MediaType;
import org.mule.runtime.api.metadata.TypedValue;
import org.mule.runtime.core.api.InternalEvent;
import org.mule.runtime.core.api.processor.MessageProcessorChain;
import org.mule.runtime.core.api.processor.Processor;
import org.mule.runtime.core.api.processor.ReactiveProcessor;
import org.mule.runtime.core.api.processor.Scope;
import org.mule.runtime.core.api.routing.ForkJoinStrategy;
import org.mule.runtime.core.api.routing.ForkJoinStrategyFactory;
import org.mule.runtime.core.api.routing.MessageSequence;
import org.mule.runtime.core.api.util.collection.SplittingStrategy;
import org.mule.runtime.core.internal.routing.forkjoin.CollectListForkJoinStrategyFactory;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;

/**
 * The {@code foreach} {@link Processor} allows iterating over a collection payload, or any collection obtained by an expression,
 * generating a message for each element.
 * <p>
 * The number of the message being processed is stored in {@code #[mel:variable:counter]} and the root message is store in
 * {@code #[mel:flowVars.rootMessage]}. Both variables may be renamed by means of {@link #setCounterVariableName(String)} and
 * {@link #setRootMessageVariableName(String)}.
 * <p>
 * Defining a groupSize greater than one, allows iterating over collections of elements of the specified size.
 * <p>
 * The {@link InternalEvent} sent to the next message processor is the same that arrived to foreach.
 */
public class Foreach extends AbstractForkJoinRouter implements Scope {

  public static final String ROOT_MESSAGE_PROPERTY = "rootMessage";

  private List<Processor> messageProcessors;
  private MessageProcessorChain ownedMessageProcessor;
  private int batchSize = 1;
  private String rootMessageVariableName;
  private String counterVariableName;

  private SplittingStrategy<InternalEvent, Iterator<TypedValue<?>>> strategy;
  private String expression = DEFAULT_SPIT_EXPRESSION;

  @Override
  protected Function<InternalEvent, Flux<InternalEvent>> decorate(Function<InternalEvent, Flux<InternalEvent>> processor) {
    return event -> {
      final String parentMessageProp = rootMessageVariableName != null ? rootMessageVariableName : ROOT_MESSAGE_PROPERTY;
      final Object previousCounterVar =
          event.getVariables().containsKey(counterVariableName) ? event.getVariables().get(counterVariableName).getValue() : null;
      final Object previousRootMessageVar =
          event.getVariables().containsKey(parentMessageProp) ? event.getVariables().get(parentMessageProp).getValue() : null;
      final InternalEvent.Builder requestBuilder = InternalEvent.builder(event);
      requestBuilder.addVariable(parentMessageProp, event.getMessage());
      return processor.apply(requestBuilder.build())
          .map(result -> {
            final InternalEvent.Builder responseBuilder = InternalEvent.builder(result);
            if (previousCounterVar != null) {
              responseBuilder.addVariable(counterVariableName, previousCounterVar);
            } else {
              responseBuilder.removeVariable(counterVariableName);
            }
            if (previousRootMessageVar != null) {
              responseBuilder.addVariable(parentMessageProp, previousRootMessageVar);
            } else {
              responseBuilder.removeVariable(parentMessageProp);
            }
            return responseBuilder.build();
          });
    };
  }

  @Override
  protected Publisher<ForkJoinStrategy.RoutingPair> getRoutingPairs(InternalEvent originalEvent) {
    AtomicInteger count = new AtomicInteger();
    if (batchSize > 1) {
      return Flux.fromIterable(() -> strategy.split(originalEvent)).buffer(batchSize)
          .map(createBatchedPartsEvent(originalEvent, count))
          .map(event -> of(event, ownedMessageProcessor));
    } else {
      return fromIterable(() -> strategy.split(originalEvent))
          .map(typedValue -> InternalEvent.builder(originalEvent).message(builder().payload(typedValue).build())
              .addVariable(counterVariableName, count.incrementAndGet()).build())
          .map(event -> of(event, ownedMessageProcessor));
    }
  }

  private Function<List<TypedValue<?>>, InternalEvent> createBatchedPartsEvent(InternalEvent originalEvent,
                                                                               AtomicInteger atomicInteger) {
    // This is required as we can't assume that all items in batch of parts are of same type, but if they are we should conserve
    // data type.
    return part -> {
      InternalEvent.Builder builder = InternalEvent.builder(originalEvent);
      MediaType itemMediaType = MediaType.ANY;
      Class itemType = Object.class;
      if (part.stream().map(i -> i.getDataType().getMediaType()).distinct().count() == 1) {
        itemMediaType = part.get(0).getDataType().getMediaType();
      }
      if (part.stream().map(i -> i.getDataType().getType()).distinct().count() == 1) {
        itemType = part.get(0).getDataType().getType();
      }
      builder.message(builder(originalEvent.getMessage())
          .collectionValue(part.stream().map(typedValue -> typedValue.getValue()).collect(toList()), itemType)
          .itemMediaType(itemMediaType).build());
      builder.addVariable(counterVariableName, atomicInteger.incrementAndGet());
      return builder.build();
    };
  }

  @Override
  protected List<MessageProcessorChain> getOwnedObjects() {
    return singletonList(ownedMessageProcessor);
  }

  public void setMessageProcessors(List<Processor> messageProcessors) {
    this.messageProcessors = messageProcessors;
  }

  @Override
  public void initialise() throws InitialisationException {
    ownedMessageProcessor = newChain(ofNullable(getFlowConstruct().getProcessingStrategy()), messageProcessors);
    strategy = new ExpressionSplittingStrategy(muleContext.getExpressionManager(), expression);
    super.initialise();
  }

  public void setCollectionExpression(String expression) {
    this.expression = expression;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public void setRootMessageVariableName(String rootMessageVariableName) {
    this.rootMessageVariableName = rootMessageVariableName;
  }

  public void setCounterVariableName(String counterVariableName) {
    this.counterVariableName = counterVariableName;
  }

  @Override
  protected int getDefaultMaxConcurrency() {
    return 1;
  }

  @Override
  protected boolean isDelayErrors() {
    return false;
  }

  @Override
  protected ForkJoinStrategyFactory getDefaultForkJoinStrategyFactory() {
    return new CollectListForkJoinStrategyFactory();
  }

}
