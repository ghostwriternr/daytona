/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

import { createUniversalStream } from './UniversalStream'
import { DaytonaError } from '../errors/DaytonaError'

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
  chunkTimeout = 2000,
  requireConsecutiveTermination = true,
): Promise<void> {
  let exitCheckStreak = 0
  let terminated = false

  // Create text decoder for consistent UTF-8 conversion across all environments
  const decoder = new TextDecoder('utf-8')

  try {
    // Get the stream response
    const response = await getStream()

    // Create universal stream that works across all environments
    const stream = createUniversalStream(response)

    // Process stream chunks
    for await (const chunk of stream) {
      if (terminated) break

      if (chunk && chunk.length > 0) {
        // Decode chunk to string
        const text = decoder.decode(chunk, { stream: true })
        if (text) {
          onChunk(text)
          exitCheckStreak = 0
        }
      } else {
        // Empty chunk - check if we should terminate with timeout
        const timeoutPromise = new Promise<boolean>((resolve) => setTimeout(() => resolve(false), chunkTimeout))
        const shouldEnd = await Promise.race([shouldTerminate(), timeoutPromise])

        if (shouldEnd) {
          exitCheckStreak += 1
          if (!requireConsecutiveTermination || exitCheckStreak > 1) {
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
