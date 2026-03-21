/**
 * Transport contract used by {@link Peer} to exchange signaling messages.
 *
 * Implementations are expected to route payloads by `namespace` and invoke
 * subscribed handlers when matching messages arrive.
 * 
 * @group Drivers
 */
export interface SignalingDriver {
  /**
   * Subscribe to signaling messages in a namespace.
   *
   * @param namespace Namespace segments used for message routing.
   * @param handler Callback invoked with message payload.
   */
  on(namespace: string[], handler: (data: any) => void): void;

  /**
   * Unsubscribe a previously registered namespace handler.
   *
   * @param namespace Namespace segments used for message routing.
   * @param handler Handler reference originally passed to `on`.
   */
  off(namespace: string[], handler: (data: any) => void): void;

  /**
   * Publish a signaling message to a namespace.
   *
   * @param namespace Target namespace segments.
   * @param data Signaling payload to deliver.
   */
  emit(namespace: string[], data: any): void;
}
