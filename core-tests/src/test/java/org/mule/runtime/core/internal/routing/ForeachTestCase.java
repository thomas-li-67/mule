/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.routing;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.rules.ExpectedException.none;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mule.runtime.api.message.Message.of;
import static org.mule.runtime.core.api.rx.Exceptions.checkedConsumer;
import static org.mule.test.allure.AllureConstants.RoutersFeature.ROUTERS_FEATURE;
import static org.mule.test.allure.AllureConstants.RoutersFeature.ForeachStory.FOR_EACH;
import static reactor.core.publisher.Flux.from;

import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.hamcrest.BaseMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.mule.runtime.api.event.Event;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.message.ErrorType;
import org.mule.runtime.api.message.Message;
import org.mule.runtime.api.metadata.TypedValue;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.core.api.InternalEvent;
import org.mule.runtime.core.api.exception.MessagingException;
import org.mule.runtime.core.api.expression.ExpressionRuntimeException;
import org.mule.runtime.core.api.processor.Processor;
import org.mule.runtime.core.api.processor.strategy.ProcessingStrategy;
import org.mule.runtime.core.api.routing.ForkJoinStrategy;
import org.mule.runtime.core.api.routing.ForkJoinStrategyFactory;
import org.mule.runtime.core.internal.routing.forkjoin.CollectListForkJoinStrategyFactory;
import org.mule.tck.junit4.AbstractMuleContextTestCase;
import org.mule.tck.testmodels.mule.TestMessageProcessor;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;

@Feature(ROUTERS_FEATURE)
@Story(FOR_EACH)
public class ForeachTestCase extends AbstractMuleContextTestCase {

  protected Foreach foreach;
  protected Foreach nestedForeach;
  protected Foreach customForeach;
  protected ArrayList<InternalEvent> processedEvents;

  private static String BAR = "bar";
  private static String ZIP = "zip";
  private static String SUFFIX = "zas";
  private static String SUFFIX_TO_ASSERT = ":zas";

  private ForkJoinStrategyFactory mockForkJoinStrategyFactory = mock(ForkJoinStrategyFactory.class);

  @Rule
  public ExpectedException expectedException = none();

  @Before
  public void setup() throws MuleException {
    processedEvents = new ArrayList<>();
    foreach =
        createForeach(underConstruction -> underConstruction
            .setMessageProcessors(singletonList(new TestMessageProcessor(SUFFIX))));
    nestedForeach =
        createForeach(checkedConsumer(underConstruction -> underConstruction.setMessageProcessors(getNestedMessageProcessors())));
  }

  @After
  public void tearDown() throws MuleException {
    foreach.dispose();
    nestedForeach.dispose();
    if (customForeach != null) {
      customForeach.dispose();
    }
  }

  @Test
  @Description("Foreach handles list payloads.")
  public void listPayload() throws Exception {
    processAndAssertBarZip(foreach, of(asList(BAR, ZIP)));
  }

  @Test
  @Description("Foreach handles array payloads.")
  public void arrayPayload() throws Exception {
    processAndAssertBarZip(foreach, of(new String[] {BAR, ZIP}));
  }

  @Test
  @Description("Foreach handles list of message collections by flattening their values.")
  public void muleMessageCollectionPayload() throws Exception {
    customForeach = createForeach(foreach -> foreach.setCollectionExpression("payload.*payload"));
    processAndAssertBarZip(customForeach, of(asList(of(BAR), of(ZIP))));
  }

  @Test
  @Description("Foreach handles iterable payloads.")
  public void iterablePayload() throws Exception {
    processAndAssertBarZip(foreach, of(new DummySimpleIterableClass()));
  }

  @Test
  @Description("Foreach handles iterator payloads.")
  public void iteratorPayload() throws Exception {
    processAndAssertBarZip(foreach, of(new DummySimpleIterableClass().iterator()));
  }

  @Test
  @Description("Source collection can come from a variable.")
  public void customCollectionLocation() throws Exception {
    final String collectionVarName = "collectionVar";
    customForeach = createForeach(underConstruction -> underConstruction.setCollectionExpression("vars." + collectionVarName));

    processAndAssertBarZip(customForeach,
                           eventBuilder().message(of(null)).addVariable(collectionVarName, new String[] {BAR, ZIP}).build(),
                           event -> event.getMessage().getPayload());
  }

  @Test
  @Description("RoutingPairs are created for each split part. Each RoutingPair has the same nested route.")
  public void routingPairs() throws Exception {
    String[] array = new String[] {BAR, ZIP};
    InternalEvent event = eventBuilder().message(of(array)).build();

    List<ForkJoinStrategy.RoutingPair> routingPairs = from(foreach.getRoutingPairs(event)).collectList().block();
    assertThat(routingPairs, hasSize(2));
    assertThat(routingPairs.get(0).getEvent().getMessage().getPayload().getValue(), equalTo(array[0]));
    assertThat(routingPairs.get(1).getEvent().getMessage().getPayload().getValue(), equalTo(array[1]));
    assertThat(routingPairs.get(0).getRoute(), equalTo(foreach.getOwnedObjects().get(0)));
    assertThat(routingPairs.get(1).getRoute(), equalTo(foreach.getOwnedObjects().get(0)));
  }

  @Test
  @Description("When a custom target is configured the router result is set in a variable and the input event is output")
  public void customTargetMessage() throws Exception {
    final String varName = "foo";
    customForeach = createForeach(foreach -> {
      foreach.setTarget(varName);
      foreach.setTargetValue("message");
    });

    processAndAssertBarZip(customForeach, eventBuilder().message(of(new String[] {BAR, ZIP})).build(),
                           event -> ((Message) event.getVariables().get(varName).getValue()).getPayload());
  }

  @Test
  @Description("When a custom target is configured the router result is set in a variable and the input event is output.")
  public void customTargetDefaultPayload() throws Exception {
    final String varName = "foo";
    customForeach = createForeach(underConstruction -> underConstruction.setTarget(varName));

    processAndAssertBarZip(customForeach, eventBuilder().message(of(new String[] {BAR, ZIP})).build(),
                           event -> event.getVariables().get(varName));
  }

  @Test
  @Description("The router uses a fork-join strategy with concurrency and timeout configured via the router and delayErrors true.")
  public void defaultForkJoinStrategyConfiguration() throws Exception {
    customForeach = createForeach(underConstruction -> underConstruction.setForkJoinStrategyFactory(mockForkJoinStrategyFactory));

    verify(mockForkJoinStrategyFactory).createForkJoinStrategy(any(ProcessingStrategy.class), eq(1), eq(false),
                                                               eq(Long.MAX_VALUE),
                                                               any(Scheduler.class), any(ErrorType.class));
  }

  @Test
  @Description("The router uses a fork-join strategy with concurrency and timeout configured via the router and delayErrors true.")
  public void customizedForkJoinStrategyConfiguration() throws Exception {
    final int concurrency = 3;
    final long timeout = 123;

    customForeach = createForeach(foreach -> {
      foreach.setMaxConcurrency(3);
      foreach.setTimeout(timeout);
      foreach.setForkJoinStrategyFactory(mockForkJoinStrategyFactory);
    });

    verify(mockForkJoinStrategyFactory).createForkJoinStrategy(any(ProcessingStrategy.class), eq(concurrency), eq(false),
                                                               eq(timeout),
                                                               any(Scheduler.class), any(ErrorType.class));
  }

  @Test
  @Description("By default CollectListForkJoinStrategyFactory is used which aggregates routes into a message with a Map<Message> payload.")
  public void defaultForkJoinStrategyFactory() throws Exception {
    assertThat(foreach.getDefaultForkJoinStrategyFactory(), instanceOf(CollectListForkJoinStrategyFactory.class));
  }

  @Test
  @Description("Delay errors is always true for foreach currently.")
  public void defaultDelayErrors() throws Exception {
    assertThat(foreach.isDelayErrors(), is(false));
  }

  @Test
  public void batchSize() throws Exception {
    customForeach = createForeach(underConstruction -> underConstruction.setBatchSize(2));

    Message result = process(customForeach, eventBuilder().message(of(asList(1, 2, 3))).build()).getMessage();

    assertThat(result.getPayload().getValue(), instanceOf(List.class));
    List<Message> list = (List<Message>) result.getPayload().getValue();

    assertThat(list, hasSize(2));
    assertThat(list.get(0).getPayload().getValue().toString(), is("[1, 2]:zas"));
    assertThat(list.get(1).getPayload().getValue().toString(), is("[3]:zas"));
  }

  @Test
  public void nestedArrayListPayload() throws Exception {
    List<List<String>> payload = new ArrayList<>();
    List<String> elem1 = new ArrayList<>();
    List<String> elem2 = new ArrayList<>();
    elem1.add("a1");
    elem1.add("a2");
    elem1.add("a3");
    elem2.add("b1");
    elem2.add("b2");
    elem2.add("b3");
    payload.add(elem1);
    payload.add(elem2);

    processAndAssertNested(nestedForeach, of(payload));
  }

  @Test
  public void nestedArrayPayload() throws Exception {
    String[][] payload = new String[2][3];
    payload[0][0] = "a1";
    payload[0][1] = "a2";
    payload[0][2] = "a3";
    payload[1][0] = "b1";
    payload[1][1] = "b2";
    payload[1][2] = "b3";

    processAndAssertNested(nestedForeach, of(payload));
  }

  @Test
  public void nestedIterablePayload() throws Exception {
    processAndAssertNested(nestedForeach, of(new DummyNestedIterableClass()));
  }

  @Test
  public void nestedIteratorPayload() throws Exception {
    processAndAssertNested(nestedForeach, of(new DummyNestedIterableClass().iterator()));
  }

  @Test
  public void failingNestedProcessor() throws Exception {
    RuntimeException throwable = new BufferOverflowException();

    Processor failingProcessor = event -> {
      throw throwable;
    };
    customForeach = createForeach(underConstruction -> underConstruction.setMessageProcessors(singletonList(failingProcessor)));

    expectedException.expect(instanceOf(MessagingException.class));
    expectedException.expect(new FailingProcessorMatcher(failingProcessor));
    expectedException.expectCause(is(throwable));
    process(customForeach, eventBuilder().message(of(new DummyNestedIterableClass().iterator())).build());
  }

  @Test
  public void failingExpression() throws Exception {
    customForeach = createForeach(underConstruction -> underConstruction.setCollectionExpression("!@INVALID"));

    expectedException.expect(instanceOf(MessagingException.class));
    expectedException.expect(new FailingProcessorMatcher(customForeach));
    expectedException.expectCause(instanceOf(ExpressionRuntimeException.class));
    process(customForeach, eventBuilder().message(of(new DummyNestedIterableClass().iterator())).build());
  }

  protected void processAndAssertBarZip(Foreach foreach, Message inMessage) throws Exception {
    processAndAssertBarZip(foreach, eventBuilder().message(inMessage).build(), null);
  }

  protected void processAndAssertBarZip(Foreach foreach, InternalEvent inEvent, Function<Event, TypedValue> resultExtractor)
      throws Exception {
    Event resultEvent = process(foreach, inEvent);

    if (resultExtractor == null) {
      resultExtractor = event -> event.getMessage().getPayload();
    } else {
      assertThat(resultEvent.getVariables(), not(equalTo(inEvent.getMessage())));
    }

    assertThat(resultEvent.getVariables().keySet(), not(hasItem("counter")));
    assertThat(resultEvent.getVariables().keySet(), not(hasItem("rootMessage")));

    TypedValue result = resultExtractor.apply(resultEvent);


    assertThat(result.getValue(), instanceOf(List.class));
    List<Message> list = (List<Message>) result.getValue();
    assertThat(list, hasSize(2));
    assertThat(list.get(0).getPayload().getValue(), equalTo(BAR + SUFFIX_TO_ASSERT));
    assertThat(list.get(1).getPayload().getValue(), equalTo(ZIP + SUFFIX_TO_ASSERT));
  }

  protected void processAndAssertNested(Foreach foreach, Message inMessage) throws Exception {
    Event resultEvent = process(foreach, eventBuilder().message(inMessage).build());

    assertThat(resultEvent.getVariables().keySet(), not(hasItem("counter")));
    assertThat(resultEvent.getVariables().keySet(), not(hasItem("rootMessage")));

    TypedValue result = resultEvent.getMessage().getPayload();

    assertThat(result.getValue(), instanceOf(List.class));
    List<Message> list = (List<Message>) result.getValue();
    assertThat(list, hasSize(2));
    assertThat(list.get(0).getPayload().getValue(), instanceOf(List.class));
    List<Message> list1 = (List<Message>) list.get(0).getPayload().getValue();
    assertThat(list1.get(0).getPayload().getValue(), equalTo("a1" + SUFFIX_TO_ASSERT));
    assertThat(list1.get(1).getPayload().getValue(), equalTo("a2" + SUFFIX_TO_ASSERT));
    assertThat(list1.get(2).getPayload().getValue(), equalTo("a3" + SUFFIX_TO_ASSERT));
    List<Message> list2 = (List<Message>) list.get(1).getPayload().getValue();
    assertThat(list2.get(0).getPayload().getValue(), equalTo("b1" + SUFFIX_TO_ASSERT));
    assertThat(list2.get(1).getPayload().getValue(), equalTo("b2" + SUFFIX_TO_ASSERT));
    assertThat(list2.get(2).getPayload().getValue(), equalTo("b3" + SUFFIX_TO_ASSERT));
  }

  private List<Processor> getNestedMessageProcessors() throws MuleException {
    List<Processor> lmp = new ArrayList<>();
    Foreach internalForeach = new Foreach();
    internalForeach.setAnnotations(getAppleFlowComponentLocationAnnotations());
    internalForeach.setMessageProcessors(singletonList(new TestMessageProcessor(SUFFIX)));
    lmp.add(internalForeach);
    return lmp;
  }

  private Foreach createForeach(Consumer<Foreach> customizer) throws MuleException {
    Foreach foreachMp = new Foreach();
    foreachMp.setMuleContext(muleContext);
    foreachMp.setAnnotations(getAppleFlowComponentLocationAnnotations());
    foreachMp.setMessageProcessors(singletonList(new TestMessageProcessor(SUFFIX)));
    customizer.accept(foreachMp);
    muleContext.getInjector().inject(foreachMp);
    foreachMp.initialise();
    return foreachMp;
  }

  // private void assertNestedProcessedMessages() {
  // String[] expectedOutputs = {"a1:foo:zas", "a2:foo:zas", "a3:foo:zas", "b1:foo:zas", "b2:foo:zas", "c1:foo:zas"};
  // assertEquals(ERR_NUMBER_MESSAGES, 6, processedEvents.size());
  // for (int i = 0; i < processedEvents.size(); i++) {
  // assertTrue(ERR_PAYLOAD_TYPE, processedEvents.get(i).getMessage().getPayload().getValue() instanceof String);
  // }
  // for (int i = 0; i < processedEvents.size(); i++) {
  // assertEquals(ERR_OUTPUT, expectedOutputs[i], processedEvents.get(i).getMessage().getPayload().getValue());
  // }
  // }

  public class DummySimpleIterableClass implements Iterable<String> {

    public List<String> strings = new ArrayList<>();

    public DummySimpleIterableClass() {
      strings.add(BAR);
      strings.add(ZIP);
    }

    @Override
    public Iterator<String> iterator() {
      return strings.iterator();
    }
  }

  private class DummyNestedIterableClass implements Iterable<DummySimpleIterableClass> {

    private List<DummySimpleIterableClass> iterables = new ArrayList<>();

    public DummyNestedIterableClass() {
      DummySimpleIterableClass dsi1 = new DummySimpleIterableClass();
      dsi1.strings = new ArrayList<>();
      dsi1.strings.add("a1");
      dsi1.strings.add("a2");
      dsi1.strings.add("a3");
      DummySimpleIterableClass dsi2 = new DummySimpleIterableClass();
      dsi2.strings = new ArrayList<>();
      dsi2.strings.add("b1");
      dsi2.strings.add("b2");
      dsi2.strings.add("b3");
      iterables.add(dsi1);
      iterables.add(dsi2);
    }

    @Override
    public Iterator<DummySimpleIterableClass> iterator() {
      return iterables.iterator();
    }
  }

  private static class FailingProcessorMatcher extends BaseMatcher<MessagingException> {

    private Processor expectedFailingProcessor;

    FailingProcessorMatcher(Processor processor) {
      this.expectedFailingProcessor = processor;
    }

    @Override
    public boolean matches(Object o) {
      return o instanceof MessagingException && ((MessagingException) o).getFailingMessageProcessor() == expectedFailingProcessor;
    }

    @Override
    public void describeTo(org.hamcrest.Description description) {
      description.appendText("Exception is not a MessagingException or failing processor does not match.");
    }
  }

}
