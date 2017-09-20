/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.test.module.extension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.core.IsSame.sameInstance;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.core.api.event.BaseEvent;
import org.mule.runtime.core.api.exception.MessagingException;
import org.mule.runtime.core.api.util.ClassUtils;
import org.mule.tck.junit4.rule.SystemProperty;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ScopeExecutionTestCase extends AbstractExtensionFunctionalTestCase {

  private static final String KILL_REASON = "I'm the one who knocks";

  @Rule
  public SystemProperty maxRedelivery = new SystemProperty("killingReason", KILL_REASON);

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Override
  protected String[] getConfigFiles() {
    return new String[] {"heisenberg-scope-config.xml"};
  }

  @Override
  protected boolean isDisposeContextPerClass() {
    return true;
  }

  @Test
  public void verifyProcessorInitialise() throws Exception {
    runFlow("getChain").getMessage().getPayload().getValue();
    runFlow("getChain").getMessage().getPayload().getValue();
    runFlow("getChain").getMessage().getPayload().getValue();
    int value = (int) runFlow("getCounter").getMessage().getPayload().getValue();
    assertThat(value, is(1));
  }

  @Test
  public void verifySameProcessorInstance() throws Exception {
    Object getChainFirst = runFlow("getChain").getMessage().getPayload().getValue();
    Object getChainSecond = runFlow("getChain").getMessage().getPayload().getValue();
    assertThat(getChainFirst, is(not(sameInstance(getChainSecond))));

    Object firstChain = ClassUtils.getFieldValue(getChainFirst, "chain", false);
    Object secondChain = ClassUtils.getFieldValue(getChainSecond, "chain", false);
    assertThat(firstChain, is(sameInstance(secondChain)));
  }

  @Test
  public void alwaysFailsWrapperFailure() throws Exception {
    expectedException.expect(instanceOf(MessagingException.class));
    // Exceptions are converted in the extension's exception enricher
    expectedException.expectCause(instanceOf(ConnectionException.class));
    expectedException.expectMessage("ON_ERROR_ERROR");
    runFlow("alwaysFailsWrapperFailure");
  }

  @Test
  public void alwaysFailsWrapperSuccess() throws Exception {
    expectedException.expect(instanceOf(MessagingException.class));
    // Exceptions are converted in the extension's exception enricher
    expectedException.expectCause(instanceOf(ConnectionException.class));
    expectedException.expectMessage("ON_SUCCESS_ERROR");
    runFlow("alwaysFailsWrapperSuccess");
  }

  @Test
  public void exceptionOnCallbacksSuccess() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    // When an exception occurs in the "onSuccess", we then invoke the onError
    expectedException.expectMessage("ON_ERROR_EXCEPTION");
    runFlow("exceptionOnCallbacksSuccess");
  }

  @Test
  public void exceptionOnCallbacksFailure() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("ON_ERROR_EXCEPTION");
    runFlow("exceptionOnCallbacksFailure");
  }

  /*
  <flow name="eachSuccess">
        <heisenberg:handle-each handleSuccess="true">
            <set-payload value="1"/>
            <set-variable variableName="myVar" value="3"/>
            <set-payload value="2"/>
        </heisenberg:handle-each>
    </flow>
  
    <flow name="eachSuccessFailPropagate">
        <heisenberg:handle-each handleSuccess="true">
            <set-payload value="FAIL"/>
            <set-variable variableName="myVar" value="3"/>
            <set-payload value="2"/>
        </heisenberg:handle-each>
    </flow>
  
    <flow name="eachSuccessFailResume">
        <heisenberg:handle-each handleSuccess="true" handleError="true" onErrorResume="true">
            <set-payload value="FAIL"/>
            <set-variable variableName="myVar" value="3"/>
            <set-payload value="FAIL"/>
        </heisenberg:handle-each>
    </flow>
  
    <flow name="eachErrorResume">
        <heisenberg:handle-each handleError="true" onErrorResume="true">
            <set-payload value="#[invalidOne]"/>
            <set-variable variableName="myVar" value="3"/>
            <set-payload value="#[invalidTwo]"/>
        </heisenberg:handle-each>
    </flow>
  
    <flow name="eachErrorPropagate">
        <heisenberg:handle-each handleError="true">
            <set-payload value="#[invalidOne]"/>
            <set-variable variableName="myVar" value="3"/>
            <set-payload value="#[invalidTwo]"/>
        </heisenberg:handle-each>
    </flow>
   */

  @Test
  public void eachErrorResume() throws Exception {
    BaseEvent event = flowRunner("eachErrorResume").run();
    assertThat(event.getVariables().get("myVar").getValue(), is("3"));
    assertThat((List<String>) event.getMessage().getPayload().getValue(),
               contains("\"Script 'invalidOne ' has errors: \n\tUnable to resolve reference of invalidOne. at 1 : 1\" evaluating expression: \"invalidOne\".",
                        "\"Script 'invalidTwo ' has errors: \n\tUnable to resolve reference of invalidTwo. at 1 : 1\" evaluating expression: \"invalidTwo\"."));
  }

  @Test
  public void eachErrorPropagate() throws Exception {
    BaseEvent event = flowRunner("eachErrorPropagate").run();
    assertThat(event.getVariables().get("myVar"), is(nullValue()));
    assertThat((List<String>) event.getMessage().getPayload().getValue(),
               contains("\"Script 'invalidOne ' has errors: \n\tUnable to resolve reference of invalidOne. at 1 : 1\" evaluating expression: \"invalidOne\"."));
  }

  @Test
  public void eachSuccess() throws Exception {
    BaseEvent event = flowRunner("eachSuccess").run();
    assertThat(event.getVariables().get("myVar").getValue(), is("3"));
    assertThat((List<String>) event.getMessage().getPayload().getValue(), contains("1", "1", "2"));
  }

  @Test
  public void eachSuccessFailResume() throws Exception {
    BaseEvent event = flowRunner("eachSuccessFailResume").run();
    assertThat(event.getVariables().get("myVar").getValue(), is("3"));
    assertThat((List<String>) event.getMessage().getPayload().getValue(), contains("FAIL", "FAIL", "FAIL"));
  }

  @Test
  public void eachSuccessFailPropagate() throws Exception {
    BaseEvent event = flowRunner("eachSuccessFailPropagate").run();
    assertThat(event.getVariables().get("myVar"), is(nullValue()));
    assertThat((List<String>) event.getMessage().getPayload().getValue(), contains("FAIL"));
  }

  @Test
  public void anything() throws Exception {
    BaseEvent event = flowRunner("executeAnything")
        .withPayload("Killed the following because I'm the one who knocks:").run();
    String expected = "Killed the following because I'm the one who knocks:";

    assertThat(event.getMessage().getPayload().getValue(), is(expected));
  }

  @Test
  public void neverFailsWrapperFailingChain() throws Exception {
    BaseEvent event = flowRunner("neverFailsWrapperFailingChain").run();

    assertThat(event.getMessage().getPayload().getValue(), is("ERROR"));
    assertThat(event.getVariables().get("varName").getValue(), is("varValue"));
  }

  @Test
  public void neverFailsWrapperSuccessChain() throws Exception {
    BaseEvent event = flowRunner("neverFailsWrapperSuccessChain")
        .withVariable("newpayload", "newpayload2")
        .run();

    assertThat(event.getMessage().getPayload().getValue(), is("SUCCESS"));
    assertThat(event.getVariables().get("varName").getValue(), is("varValue"));
  }

  @Test
  public void payloadModifier() throws Exception {
    BaseEvent event = flowRunner("payloadModifier").run();

    assertThat(event.getMessage().getPayload().getValue(), is("MESSAGE"));
    assertThat(event.getVariables().get("newPayload").getValue(), is("MESSAGE"));
    assertThat(event.getVariables().get("newAttributes").getValue(), is(notNullValue()));
  }

  @Test
  public void neverFailsWrapperNoChain() throws Exception {
    BaseEvent event = flowRunner("neverFailsWrapperNoChain").run();

    assertThat(event.getMessage().getPayload().getValue(), is("EMPTY"));
  }

}
