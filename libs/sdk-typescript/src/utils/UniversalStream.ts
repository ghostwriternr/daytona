/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

import { DaytonaError } from '../errors/DaytonaError'
import { RuntimeEnvironment, isNodeStream, isAsyncIterable, isReadableStream } from './runtime'

/**
 * Type definition for Node.js readable streams with event emitter methods
 */
interface NodeReadableStream extends NodeJS.ReadableStream {
  on(event: 'data', listener: (chunk: Buffer | string) => void): this
  on(event: 'end', listener: () => void): this
  on(event: 'error', listener: (error: Error) => void): this
  on(event: 'close', listener: () => void): this
  on(event: string, listener: (...args: unknown[]) => void): this

  once(event: 'data', listener: (chunk: Buffer | string) => void): this
  once(event: 'end', listener: () => void): this
  once(event: 'error', listener: (error: Error) => void): this
  once(event: string, listener: (...args: unknown[]) => void): this

  off(event: 'data', listener: (chunk: Buffer | string) => void): this
  off(event: 'end', listener: () => void): this
  off(event: 'error', listener: (error: Error) => void): this
  off(event: string, listener: (...args: unknown[]) => void): this

  destroy(error?: Error): this
  readable: boolean
}

/**
 * Universal stream interface that works across all JavaScript runtimes
 */
export interface UniversalStream {
  /**
   * Async iteration support for consuming stream data
   */
  [Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array>

  /**
   * Cancel/cleanup the stream
   */
  cancel(): Promise<void>

  /**
   * Check if stream is still readable
   */
  readonly readable: boolean
}

/**
 * Options for creating universal streams
 */
export interface UniversalStreamOptions {
  encoding?: 'utf8' | 'binary'
}

/**
 * Adapter for Web Streams API (Cloudflare Workers, Browsers, Deno)
 */
export class WebStreamAdapter implements UniversalStream {
  private reader: ReadableStreamDefaultReader<Uint8Array> | null = null
  private stream: ReadableStream<Uint8Array>
  private _cancelled = false

  constructor(stream: ReadableStream<Uint8Array>, _options?: UniversalStreamOptions) {
    this.stream = stream
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    if (this._cancelled) {
      throw new DaytonaError('Stream has been cancelled', 'STREAM_CANCELLED')
    }

    this.reader = this.stream.getReader()

    try {
      while (true) {
        const { done, value } = await this.reader.read()
        if (done) break
        if (value && value.length > 0) {
          yield value
        }
      }
    } finally {
      if (this.reader) {
        this.reader.releaseLock()
        this.reader = null
      }
    }
  }

  async cancel(): Promise<void> {
    this._cancelled = true
    if (this.reader) {
      await this.reader.cancel()
      this.reader.releaseLock()
      this.reader = null
    } else if (!this.stream.locked) {
      await this.stream.cancel()
    }
  }

  get readable(): boolean {
    return !this._cancelled && !this.stream.locked
  }
}

/**
 * Adapter for Node.js streams (backward compatibility)
 */
export class NodeStreamAdapter implements UniversalStream {
  private destroyed = false
  private stream: NodeReadableStream

  constructor(stream: NodeReadableStream, _options?: UniversalStreamOptions) {
    this.stream = stream
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    if (this.destroyed) {
      throw new DaytonaError('Stream has been destroyed', 'STREAM_DESTROYED')
    }

    // Use Node.js native async iteration if available
    if (Symbol.asyncIterator in this.stream && typeof this.stream[Symbol.asyncIterator] === 'function') {
      const iterableStream = this.stream as AsyncIterable<Buffer | string | Uint8Array>
      for await (const chunk of iterableStream) {
        if (this.destroyed) break
        yield this.toUint8Array(chunk)
      }
      return
    }

    // Fallback for older Node.js versions - TypeScript needs help understanding the type here
    const stream = this.stream as NodeReadableStream
    const chunks: Uint8Array[] = []
    let resolve: ((value: IteratorResult<Uint8Array>) => void) | null = null
    let reject: ((error: Error) => void) | null = null

    const onData = (chunk: Buffer | string | Uint8Array) => {
      const uint8Chunk = this.toUint8Array(chunk)
      if (resolve) {
        resolve({ done: false, value: uint8Chunk })
        resolve = null
        reject = null
      } else {
        chunks.push(uint8Chunk)
      }
    }

    const onEnd = () => {
      if (resolve) {
        resolve({ done: true, value: undefined })
      }
      cleanup()
    }

    const onError = (err: Error) => {
      if (reject) {
        reject(err)
      }
      cleanup()
    }

    const cleanup = () => {
      stream.off('data', onData)
      stream.off('end', onEnd)
      stream.off('error', onError)
    }

    stream.on('data', onData)
    stream.on('end', onEnd)
    stream.on('error', onError)

    try {
      while (!this.destroyed) {
        if (chunks.length > 0) {
          const chunk = chunks.shift()
          if (chunk) yield chunk
        } else {
          const result = await new Promise<IteratorResult<Uint8Array>>((res, rej) => {
            resolve = res
            reject = rej
          })
          if (result.done) break
          if (result.value) yield result.value
        }
      }
    } finally {
      cleanup()
    }
  }

  private toUint8Array(chunk: Buffer | string | Uint8Array | ArrayBuffer | ArrayBufferView): Uint8Array {
    if (chunk instanceof Uint8Array) return chunk

    if (typeof chunk === 'string') {
      return new TextEncoder().encode(chunk)
    }

    // Handle Buffer in Node.js
    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(chunk)) {
      return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength)
    }

    // Handle ArrayBuffer
    if (chunk instanceof ArrayBuffer) {
      return new Uint8Array(chunk)
    }

    // Handle other typed arrays
    if (ArrayBuffer.isView(chunk)) {
      return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength)
    }

    throw new DaytonaError(
      `Unexpected chunk type: ${typeof chunk}. Expected Uint8Array, string, or Buffer`,
      'INVALID_CHUNK_TYPE',
    )
  }

  async cancel(): Promise<void> {
    this.destroyed = true
    if ('destroy' in this.stream && typeof this.stream.destroy === 'function') {
      this.stream.destroy()
    } else if (
      'close' in this.stream &&
      typeof (this.stream as NodeReadableStream & { close(): void }).close === 'function'
    ) {
      ;(this.stream as NodeReadableStream & { close(): void }).close()
    }
  }

  get readable(): boolean {
    return !this.destroyed && 'readable' in this.stream && this.stream.readable !== false
  }
}

/**
 * Adapter for async iterables
 */
export class AsyncIterableAdapter<T = Buffer | string | Uint8Array> implements UniversalStream {
  private cancelled = false
  private iterator: AsyncIterator<T> | null = null

  constructor(
    private iterable: AsyncIterable<T>,
    private options?: UniversalStreamOptions,
  ) {}

  async *[Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    if (this.cancelled) {
      throw new DaytonaError('Stream has been cancelled', 'STREAM_CANCELLED')
    }

    this.iterator = this.iterable[Symbol.asyncIterator]()

    try {
      while (!this.cancelled) {
        const { done, value } = await this.iterator.next()
        if (done) break
        if (value) {
          yield this.toUint8Array(value)
        }
      }
    } finally {
      if (this.iterator && typeof this.iterator.return === 'function') {
        await this.iterator.return()
      }
    }
  }

  private toUint8Array(value: T): Uint8Array {
    if (value instanceof Uint8Array) return value

    if (typeof value === 'string') {
      return new TextEncoder().encode(value)
    }

    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
      return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
    }

    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value)
    }

    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
    }

    throw new DaytonaError(`Unexpected value type in async iterable: ${typeof value}`, 'INVALID_ITERABLE_VALUE')
  }

  async cancel(): Promise<void> {
    this.cancelled = true
    if (this.iterator && typeof this.iterator.return === 'function') {
      await this.iterator.return()
    }
  }

  get readable(): boolean {
    return !this.cancelled
  }
}

/**
 * Factory function to create a universal stream from various sources
 */
export function createUniversalStream(source: unknown, options?: UniversalStreamOptions): UniversalStream {
  // Handle null/undefined
  if (!source) {
    throw new DaytonaError('Stream source is null or undefined', 'INVALID_STREAM_SOURCE')
  }

  // Web Streams API (preferred for web environments)
  if (isReadableStream(source)) {
    return new WebStreamAdapter(source as ReadableStream<Uint8Array>, options)
  }

  // Axios response object with body property (Cloudflare Workers, fetch responses)
  if (typeof source === 'object' && 'body' in source) {
    const body = (source as { body: unknown }).body
    if (isReadableStream(body)) {
      return new WebStreamAdapter(body, options)
    }
  }

  // Node.js streams (for Node.js environments)
  if (RuntimeEnvironment.isNode() && isNodeStream(source)) {
    return new NodeStreamAdapter(source as NodeReadableStream, options)
  }

  // Async iterables
  if (isAsyncIterable(source)) {
    return new AsyncIterableAdapter(source, options)
  }

  // If we have a data property, check if it's a stream (Axios response pattern)
  if (typeof source === 'object' && 'data' in source) {
    return createUniversalStream((source as { data: unknown }).data, options)
  }

  throw new DaytonaError(
    `Unsupported stream type. Expected ReadableStream, Node.js Stream, or AsyncIterable. ` +
      `Got: ${source?.constructor?.name || typeof source}. ` +
      `Runtime: ${RuntimeEnvironment.detect()}`,
    'UNSUPPORTED_STREAM_TYPE',
  )
}

/**
 * Helper function to handle timeout with async operations
 */
export async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, timeoutValue?: T): Promise<T> {
  const timeoutPromise = new Promise<T>((resolve) => setTimeout(() => resolve(timeoutValue as T), timeoutMs))
  return Promise.race([promise, timeoutPromise])
}
