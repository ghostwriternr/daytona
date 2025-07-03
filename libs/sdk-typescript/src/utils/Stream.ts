/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

import { createUniversalStream, withTimeout } from './UniversalStream'
import { DaytonaError } from '../errors/DaytonaError'

/**
 * Options for processing streaming responses
 */
export interface StreamProcessingOptions {
  /**
   * Timeout for each chunk in milliseconds
   */
  chunkTimeout?: number

  /**
   * Whether to require consecutive termination signals to terminate the stream
   */
  requireConsecutiveTermination?: boolean

  /**
   * Text encoding for decoding chunks
   */
  encoding?: 'utf8' | 'utf-8' | 'binary'

  /**
   * Optional abort signal for cancellation
   */
  signal?: AbortSignal
}

/**
 * Process a streaming response from a URL. Stream will terminate if the server-side stream
 * ends or if the shouldTerminate function returns True.
 *
 * This function now works across all JavaScript runtimes including Node.js, Cloudflare Workers,
 * Deno, and browsers by using a universal stream abstraction.
 *
 * @param getStream - A function that returns a promise of a response with stream data
 * @param onChunk - A function to process each chunk of the response
 * @param shouldTerminate - A function to check if the response should be terminated
 * @param chunkTimeout - The timeout for each chunk (default: 2000ms)
 * @param requireConsecutiveTermination - Whether to require two consecutive termination signals
 * to terminate the stream (default: true)
 */
export async function processStreamingResponse(
  getStream: () => Promise<unknown>,
  onChunk: (chunk: string) => void,
  shouldTerminate: () => Promise<boolean>,
  chunkTimeout?: number,
  requireConsecutiveTermination?: boolean,
): Promise<void>

/**
 * Process a streaming response with additional options
 */
export async function processStreamingResponse(
  getStream: () => Promise<unknown>,
  onChunk: (chunk: string) => void,
  shouldTerminate: () => Promise<boolean>,
  options: StreamProcessingOptions,
): Promise<void>

export async function processStreamingResponse(
  getStream: () => Promise<unknown>,
  onChunk: (chunk: string) => void,
  shouldTerminate: () => Promise<boolean>,
  chunkTimeoutOrOptions: number | StreamProcessingOptions = 2000,
  requireConsecutiveTermination = true,
): Promise<void> {
  // Handle overloaded parameters
  const options: StreamProcessingOptions =
    typeof chunkTimeoutOrOptions === 'number'
      ? { chunkTimeout: chunkTimeoutOrOptions, requireConsecutiveTermination }
      : chunkTimeoutOrOptions

  const {
    chunkTimeout = 2000,
    requireConsecutiveTermination: requireConsecutive = true,
    encoding = 'utf8',
    signal,
  } = options

  let exitCheckStreak = 0
  let terminated = false

  // Create text decoder for consistent string conversion
  const decoder = new TextDecoder(encoding === 'binary' ? 'latin1' : encoding)

  try {
    // Get the stream response
    const response = await getStream()

    // Create universal stream that works across all environments
    const stream = createUniversalStream(response, { encoding: encoding as 'utf8' | 'binary' })

    // Check for abort signal
    const checkAborted = () => {
      if (signal?.aborted) {
        terminated = true
        throw new DaytonaError('Stream processing aborted', 'STREAM_ABORTED')
      }
    }

    // Process stream chunks
    for await (const chunk of stream) {
      checkAborted()

      if (terminated) break

      if (chunk && chunk.length > 0) {
        // Decode chunk to string
        const text = decoder.decode(chunk, { stream: true })
        if (text) {
          onChunk(text)
          exitCheckStreak = 0
        }
      } else {
        // Empty chunk - check if we should terminate
        const shouldEnd = await withTimeout(shouldTerminate(), chunkTimeout, false)

        if (shouldEnd) {
          exitCheckStreak += 1
          if (!requireConsecutive || exitCheckStreak > 1) {
            terminated = true
            break
          }
        } else {
          exitCheckStreak = 0
        }
      }
    }

    // Flush any remaining bytes in the decoder
    const remaining = decoder.decode()
    if (remaining) {
      onChunk(remaining)
    }

    // Clean up the stream
    await stream.cancel()
  } catch (error) {
    terminated = true

    // Re-throw DaytonaError as-is
    if (error instanceof DaytonaError) {
      throw error
    }

    // Wrap other errors
    throw new DaytonaError(
      `Stream processing error: ${error instanceof Error ? error.message : String(error)}`,
      'STREAM_PROCESSING_ERROR',
    )
  }
}

/**
 * Legacy function maintained for backward compatibility
 * @deprecated Use processStreamingResponse instead
 */
export { processStreamingResponse as processStream }
