/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * The annotation denotes that the objects of the attached class will be used to build the JSON response for some endpoints
 * and some of its field keys are defined in another class which is referenced by the {@link #value()}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface JsonResponseExternalFields {
  /**
   * @return The reference class.
   */
  Class<?> value();
}
