/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * @module Errors
 */

/**
 * Base error for Daytona SDK.
 */
export class DaytonaError extends Error {
  constructor(
    message: string,
    public code?: string,
  ) {
    super(message)
    this.name = 'DaytonaError'
  }
}

export class DaytonaNotFoundError extends DaytonaError {}
