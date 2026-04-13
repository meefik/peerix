/**
 * Transport contract used by {@link Peer} to exchange signaling messages.
 * 
 * @group Drivers
 */
export interface SignalingDriver {
  /**
   * Indicates whether the driver is currently active 
   * and ready to send and receive signaling messages.
   */
  active?: boolean;

  /**
   * Subscribe to a namespace for signaling messages and driver events.
   *
   * @param namespace Namespace segments used for message routing.
   * @param handler Callback invoked when a signaling message or driver event is received.
   */
  on(namespace: SignalingNamespace, handler: (message?: any) => void): Promise<void> | void;

  /**
   * Unsubscribe a previously registered namespace handler.
   *
   * @param namespace Namespace segments used for message routing.
   * @param handler Handler reference originally passed to `on`.
   */
  off(namespace: SignalingNamespace, handler: (message?: any) => void): Promise<void> | void;

  /**
   * Publish a signaling message or driver event to a namespace.
   *
   * @param namespace Target namespace segments.
   * @param message Optional message to deliver.
   */
  emit(namespace: SignalingNamespace, message?: any): Promise<void> | void;
}

/**
 * Signaling namespace segments used for message routing and driver events.
 * 
 * Possible namespaces include:
 * - `[ 'active' ]` : emitted when the signaling driver is ready to send and receive messages.
 * - `[ 'inactive' ]` : emitted when the signaling driver is no longer able to send and receive messages.
 * - `[ 'error' ]` : emitted when the driver encounters an asynchronous error.
 * - `[ 'message', 'room-id' ]` : subscribe and publish signaling messages related to a specific room.
 * - `[ 'message', 'room-id', 'peer-id' ]` : subscribe and publish signaling messages related to a specific peer in a room.
 * 
 * @group Drivers
 */
export type SignalingNamespace =
  | ['active']
  | ['inactive']
  | ['error']
  | ['message', ...string[]];
