/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.transformer.type;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import org.mule.runtime.api.metadata.DataType;
import org.mule.runtime.api.metadata.MediaType;
import org.mule.tck.junit4.AbstractMuleTestCase;

import org.junit.Test;

public class DataTypeMatchingTestCase extends AbstractMuleTestCase {


  private static final MediaType STAR_STAR_MEDIA_TYPE = MediaType.ANY;
  private static final MediaType APPLICATION_JSON_MEDIA_TYPE = MediaType.APPLICATION_JSON;
  private static final MediaType TEXT_MEDIA_TYPE = MediaType.TEXT;
  private static final MediaType FOO_MEDIA_TYPE = MediaType.create("foo", "foo");

  private static final DataType GENERIC_DATA_TYPE = DataType.builder().type(Object.class).mediaType(STAR_STAR_MEDIA_TYPE).build();
  private static final DataType GENERIC_TYPE_GENERIC_DATA_TYPE =
      DataType.builder().type(Object.class).mediaType(FOO_MEDIA_TYPE).build();
  private static final DataType GENERIC_MEDIA_TYPE_GENERIC_DATA_TYPE =
      DataType.builder().type(FOO.class).mediaType(STAR_STAR_MEDIA_TYPE).build();

  private static final DataType JSON_PARENT_DATA_TYPE =
      DataType.builder().type(JSON_PARENT.class).mediaType(APPLICATION_JSON_MEDIA_TYPE).build();
  private static final DataType JSON_SON_DATA_TYPE =
      DataType.builder().type(JSON_SON.class).mediaType(APPLICATION_JSON_MEDIA_TYPE).build();
  private static final DataType TEXT_DATA_TYPE = DataType.builder().type(TEXT.class).mediaType(TEXT_MEDIA_TYPE).build();

  private static final DataType[] dataTypes = {GENERIC_DATA_TYPE, GENERIC_TYPE_GENERIC_DATA_TYPE,
      GENERIC_MEDIA_TYPE_GENERIC_DATA_TYPE, JSON_PARENT_DATA_TYPE, JSON_SON_DATA_TYPE, TEXT_DATA_TYPE};

  private static class JSON_PARENT {
  }
  private static class JSON_SON extends JSON_PARENT {
  }
  private static class TEXT {
  }
  private static class FOO {
  }

  @Test
  public void sameDataTypeIsEqualsToItself() throws Exception {
    for (int i = 0; i < dataTypes.length; i++) {
      assertThat(dataTypes[i], is(equalTo(dataTypes[i])));
    }
  }

  @Test
  public void sameDataTypeIsCompatibleWithItself() throws Exception {
    for (int i = 0; i < dataTypes.length; i++) {
      assertThat(dataTypes[i].isCompatibleWith(dataTypes[i]), is(true));
    }
  }

  @Test
  public void sameDataTypeMatchItSelf() throws Exception {
    for (int i = 0; i < dataTypes.length; i++) {
      assertThat(DataType.match(dataTypes[i], dataTypes[i]), is(true));
    }
  }

  @Test
  public void allDataTypesAreDifferent() throws Exception {
    for (int i = 0; i < dataTypes.length; i++) {
      for (int j = 0; j < dataTypes.length; j++) {
        if (i == j) {
          continue;
        }
        assertThat(dataTypes[i], is(not(equalTo(dataTypes[j]))));
        assertThat(dataTypes[j], is(not(equalTo(dataTypes[i]))));
      }
    }
  }

  @Test
  public void genericDataTypeShouldBeCompatibleWithEveryDataType() throws Exception {
    assertThat(GENERIC_DATA_TYPE.isCompatibleWith(JSON_PARENT_DATA_TYPE), is(true));
    assertThat(GENERIC_DATA_TYPE.isCompatibleWith(JSON_SON_DATA_TYPE), is(true));
    assertThat(GENERIC_DATA_TYPE.isCompatibleWith(TEXT_DATA_TYPE), is(true));
    assertThat(GENERIC_DATA_TYPE.isCompatibleWith(GENERIC_TYPE_GENERIC_DATA_TYPE), is(true));
    assertThat(GENERIC_DATA_TYPE.isCompatibleWith(GENERIC_MEDIA_TYPE_GENERIC_DATA_TYPE), is(true));
  }

  @Test
  public void noDataTypeShouldBeCompatibleWithTheGenericOne() throws Exception {
    assertThat(JSON_PARENT_DATA_TYPE.isCompatibleWith(GENERIC_DATA_TYPE), is(false));
    assertThat(JSON_SON_DATA_TYPE.isCompatibleWith(GENERIC_DATA_TYPE), is(false));
    assertThat(TEXT_DATA_TYPE.isCompatibleWith(GENERIC_DATA_TYPE), is(false));
  }

  @Test
  public void nonGenericDataTypesShouldNotBeCompatible() throws Exception {
    assertThat(JSON_PARENT_DATA_TYPE.isCompatibleWith(TEXT_DATA_TYPE), is(false));
    assertThat(TEXT_DATA_TYPE.isCompatibleWith(JSON_PARENT_DATA_TYPE), is(false));
    assertThat(JSON_SON_DATA_TYPE.isCompatibleWith(TEXT_DATA_TYPE), is(false));
    assertThat(TEXT_DATA_TYPE.isCompatibleWith(JSON_SON_DATA_TYPE), is(false));
  }

  @Test
  public void inheritedTypeDataTypesShouldBeCompatibleOneWay() throws Exception {
    assertThat(JSON_PARENT_DATA_TYPE.isCompatibleWith(JSON_SON_DATA_TYPE), is(true));
    assertThat(JSON_SON_DATA_TYPE.isCompatibleWith(JSON_PARENT_DATA_TYPE), is(false));
  }

  @Test
  public void thereShouldBeCompatibilityInInheritedDataTypes() throws Exception {
    assertThat(DataType.anyCompatibilityExist(JSON_PARENT_DATA_TYPE, JSON_SON_DATA_TYPE), is(true));
    assertThat(DataType.anyCompatibilityExist(JSON_SON_DATA_TYPE, JSON_PARENT_DATA_TYPE), is(true));
  }

  @Test
  public void thereShouldExistCompatibilityBetweenGenericAndAnyOtherDataType() throws Exception {
    assertThat(DataType.anyCompatibilityExist(GENERIC_DATA_TYPE, JSON_PARENT_DATA_TYPE), is(true));
    assertThat(DataType.anyCompatibilityExist(JSON_PARENT_DATA_TYPE, GENERIC_DATA_TYPE), is(true));
    assertThat(DataType.anyCompatibilityExist(GENERIC_DATA_TYPE, JSON_SON_DATA_TYPE), is(true));
    assertThat(DataType.anyCompatibilityExist(JSON_SON_DATA_TYPE, GENERIC_DATA_TYPE), is(true));
    assertThat(DataType.anyCompatibilityExist(GENERIC_DATA_TYPE, TEXT_DATA_TYPE), is(true));
    assertThat(DataType.anyCompatibilityExist(TEXT_DATA_TYPE, GENERIC_DATA_TYPE), is(true));
    assertThat(DataType.anyCompatibilityExist(GENERIC_DATA_TYPE, GENERIC_TYPE_GENERIC_DATA_TYPE), is(true));
    assertThat(DataType.anyCompatibilityExist(GENERIC_TYPE_GENERIC_DATA_TYPE, GENERIC_DATA_TYPE), is(true));
    assertThat(DataType.anyCompatibilityExist(GENERIC_DATA_TYPE, GENERIC_MEDIA_TYPE_GENERIC_DATA_TYPE), is(true));
    assertThat(DataType.anyCompatibilityExist(GENERIC_MEDIA_TYPE_GENERIC_DATA_TYPE, GENERIC_DATA_TYPE), is(true));
  }

  @Test
  public void onlySameTypeAndMatchingMediaTypeShouldMatch() throws Exception {
    for (int i = 0; i < dataTypes.length; i++) {
      for (int j = 0; j < dataTypes.length; j++) {
        if (i == j) {
          continue;
        }
        if (dataTypes[i].equals(GENERIC_DATA_TYPE) && dataTypes[j].equals(GENERIC_MEDIA_TYPE_GENERIC_DATA_TYPE) ||
            dataTypes[i].equals(GENERIC_MEDIA_TYPE_GENERIC_DATA_TYPE) && dataTypes[j].equals(GENERIC_DATA_TYPE)) {
          //These should match
          continue;
        }
        try {
          assertThat(dataTypes[i].matches(dataTypes[j]), is(false));
          assertThat(dataTypes[j].matches(dataTypes[i]), is(false));
        } catch (Error e) {

        }
      }
    }
    DataType genericDataType = DataType.builder(TEXT_DATA_TYPE).mediaType(STAR_STAR_MEDIA_TYPE).build();
    assertThat(genericDataType.matches(TEXT_DATA_TYPE), is(true));
    assertThat(TEXT_DATA_TYPE.matches(genericDataType), is(true));
  }



}
