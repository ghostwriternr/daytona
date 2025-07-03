/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Runtime environment detection utilities for cross-platform compatibility
 */

/**
 * Detects the current JavaScript runtime environment
 */
export const RuntimeEnvironment = {
  /**
   * Check if running in Node.js environment
   */
  isNode(): boolean {
    return typeof process !== 'undefined' && process.versions?.node !== undefined && typeof require === 'function'
  },

  /**
   * Check if running in Cloudflare Workers environment
   */
  isCloudflareWorkers(): boolean {
    // Check for EdgeRuntime global
    if ('EdgeRuntime' in globalThis && typeof (globalThis as { EdgeRuntime?: unknown }).EdgeRuntime !== 'undefined') {
      return true
    }
    // Check for WebSocketPair global (Cloudflare Workers specific)
    if (
      'WebSocketPair' in globalThis &&
      typeof (globalThis as { WebSocketPair?: unknown }).WebSocketPair !== 'undefined'
    ) {
      return true
    }
    return false
  },

  /**
   * Check if running in Deno environment
   */
  isDeno(): boolean {
    return 'Deno' in globalThis && typeof (globalThis as { Deno?: unknown }).Deno !== 'undefined'
  },

  /**
   * Check if running in Browser environment
   */
  isBrowser(): boolean {
    return typeof window !== 'undefined' && typeof window.document !== 'undefined'
  },

  /**
   * Check if Web Streams API is available
   */
  hasWebStreams(): boolean {
    return typeof globalThis.ReadableStream === 'function' && typeof globalThis.WritableStream === 'function'
  },

  /**
   * Detect the runtime environment type
   */
  detect(): 'node' | 'cloudflare' | 'deno' | 'browser' | 'unknown' {
    if (this.isNode()) return 'node'
    if (this.isCloudflareWorkers()) return 'cloudflare'
    if (this.isDeno()) return 'deno'
    if (this.isBrowser()) return 'browser'
    return 'unknown'
  },

  /**
   * Check if environment is web-based (non-Node.js)
   */
  isWebEnvironment(): boolean {
    return !this.isNode() && this.hasWebStreams()
  },
}

/**
 * Type guard to check if an object is a Node.js stream
 */
export function isNodeStream(obj: unknown): boolean {
  return (
    obj !== null &&
    typeof obj === 'object' &&
    'on' in obj &&
    typeof obj.on === 'function' &&
    'once' in obj &&
    typeof obj.once === 'function' &&
    'off' in obj &&
    typeof obj.off === 'function' &&
    'pipe' in obj &&
    typeof obj.pipe === 'function'
  )
}

/**
 * Type guard to check if an object is async iterable
 */
export function isAsyncIterable<T = unknown>(obj: unknown): obj is AsyncIterable<T> {
  return (
    obj !== null &&
    typeof obj === 'object' &&
    Symbol.asyncIterator in obj &&
    typeof obj[Symbol.asyncIterator] === 'function'
  )
}

/**
 * Type guard to check if an object is a Web ReadableStream
 */
export function isReadableStream(obj: unknown): obj is ReadableStream {
  return (
    obj !== null &&
    typeof obj === 'object' &&
    'getReader' in obj &&
    typeof obj.getReader === 'function' &&
    'cancel' in obj &&
    typeof obj.cancel === 'function'
  )
}
