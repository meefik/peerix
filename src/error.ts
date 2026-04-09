/**
 * Error codes for categorizing Peer-related errors.
 * All codes ending in 'FAILED' indicate critical errors.
 * All codes ending in 'ERROR' indicate non-critical errors.
 * 
 * @group Errors
 */
export type ErrorCode = 'UNKNOWN_ERROR'
  | 'PEER_CONNECTION_FAILED'
  | 'PEER_SIGNALING_FAILED'
  | 'PEER_NEGOTIATION_FAILED'
  | 'PEER_ICECANDIDATE_ERROR'
  | 'PEER_MEDIASTREAM_ERROR'
  | 'PEER_DATACHANNEL_ERROR';

/**
 * PeerixError is a custom error class for Peer-related errors.
 * It extends the built-in Error class and includes additional properties like 'code'.
 * 
 * @group Errors
 */
export class PeerixError extends Error {
  /**
   * The name of the error, typically 'Error' or a specific error type.
   */
  name: string;
  /**
   * The error message providing details about the error.
   */
  message: string;
  /**
   * An error code for categorizing the error.
   */
  code: ErrorCode;
  /**
   * Constructs a new PeerixError instance.
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
    // Fix prototype chain for built-in Error
    Object.setPrototypeOf(this, PeerixError.prototype);
  }
}
