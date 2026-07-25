/**
 * Custom error types for Peerix.
 *
 * Provides {@link PeerixError}, a typed error class with categorized
 * {@link ErrorCode} values, and the {@link ErrorEvent} interface used
 * by the library's event system to report failures.
 *
 * @module Errors
 */

/**
 * Error codes for categorizing Peerix-related errors.
 */
export type ErrorCode =
  | "UNKNOWN_ERROR"
  | "SIGNALING_ERROR"
  | "NEGOTIATION_ERROR"
  | "ICECANDIDATE_ERROR"
  | "MEDIASTREAM_ERROR"
  | "DATACHANNEL_ERROR";

/**
 * Event emitted when an error occurs in any background operations.
 */
export interface ErrorEvent {
  /** Name of the event. */
  name: "error";
  /** Error object containing details about the error. */
  error: PeerixError;
}

/**
 * Custom error class for Peerix-related errors.
 * Extends the built-in Error class and adds a `code` property.
 */
export class PeerixError extends Error {
  /** The name of the error, typically `Error` or a specific error type. */
  override name: string;
  /** The error message providing details about the error. */
  override message: string;
  /** An error code for categorizing the error. */
  readonly code: ErrorCode;

  /**
   * Creates a new {@link PeerixError} instance.
   *
   * @param error An error object, string message, or any value to wrap.
   * @param code An error code for categorizing the error.
   */
  constructor(error: unknown, code?: ErrorCode) {
    const { name, message } =
      typeof error === "object" && error !== null
        ? (error as Error)
        : { message: String(error) };
    super(message);
    this.name = name || "Error";
    this.message = message || "Unknown error";
    this.code = code ?? "UNKNOWN_ERROR";
    // fix prototype chain so `instanceof` works correctly
    Object.setPrototypeOf(this, PeerixError.prototype);
  }
}
