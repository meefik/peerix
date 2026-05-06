/**
 * Error codes for categorizing Peerix-related errors.
 * 
 * @group Errors
 */
export type ErrorCode = 'UNKNOWN_ERROR'
  | 'PEER_SIGNALING_ERROR'
  | 'PEER_NEGOTIATION_ERROR'
  | 'PEER_ICECANDIDATE_ERROR'
  | 'PEER_MEDIASTREAM_ERROR'
  | 'PEER_DATACHANNEL_ERROR';

/**
 * Custom error class for Peerix-related errors.
 * Extends the built-in Error class and adds a `code` property.
 * 
 * @group Errors
 */
export class PeerixError extends Error {
  /** The name of the error, typically 'Error' or a specific error type. */
  readonly name: string;
  /** The error message providing details about the error. */
  readonly message: string;
  /** An error code for categorizing the error. */
  readonly code: ErrorCode;

  /**
   * Creates a new {@link PeerixError} instance.
   * 
   * @param error An object containing the error details: name, message.
   * @param code An error code for categorizing the error.
   */
  constructor(error: any, code?: ErrorCode) {
    const { name, message } =
      typeof error === 'object' && error !== null
        ? error : { message: String(error) };
    super(message);
    this.name = name || 'Error';
    this.message = message || 'Unknown error';
    this.code = code || 'UNKNOWN_ERROR';
    // fix the prototype chain for built-in Error
    Object.setPrototypeOf(this, PeerixError.prototype);
  }
}
