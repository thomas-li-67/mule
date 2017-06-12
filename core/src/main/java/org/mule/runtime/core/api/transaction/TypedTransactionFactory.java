/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.api.transaction;

/**
 * {@link TransactionFactory} that specifies the {@link TransactionType} it handles. Implementations should be registered via SPI.
 *
 * @since 4.0
 */
public interface TypedTransactionFactory extends TransactionFactory {

  /**
   * @return the {@link TransactionType} handled
   */
  TransactionType getType();

}
