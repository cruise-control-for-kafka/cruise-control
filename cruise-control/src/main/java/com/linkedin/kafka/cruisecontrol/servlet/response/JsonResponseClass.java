/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * The Annotation to denote that the object of the attached class will be used to build the JSON response for some endpoints.
 * The class annotated with this annotation is expected to either also be annotated with {@link JsonResponseExternalFields} or
 * have some (static) fields annotated with {@link JsonResponseField} or both. {@link JsonResponseExternalFields} and
 * {@link JsonResponseField} mark all the field keys which should be in the JSON response.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface JsonResponseClass {
}
