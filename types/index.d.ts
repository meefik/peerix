export interface Driver {
  on(namespace: string[], handler: (message: any) => void): void;
  off(namespace: string[], handler: (message: any) => void): void;
  emit(namespace: string[], message: any): void;
}

export interface ConnectEvent {
  id: string;
  peer: RTCPeerConnection;
  metadata?: any;
}

export interface StreamEvent {
  id: string;
  peer: RTCPeerConnection;
  stream: MediaStream;
  metadata?: any;
}

export interface DisposeEvent {
  id: string;
  peer: RTCPeerConnection;
  error?: Error;
}

export interface ErrorEvent {
  id: string;
  error: Error;
}

export interface ChannelOpenEvent {
  id: string;
  peer: RTCPeerConnection;
  channel: RTCDataChannel;
}

export interface ChannelCloseEvent {
  id: string;
  peer: RTCPeerConnection;
  channel: RTCDataChannel;
}

export interface ChannelErrorEvent {
  id: string;
  peer: RTCPeerConnection;
  channel: RTCDataChannel;
  error: Error;
}

export interface ChannelMessageEvent {
  id: string;
  peer: RTCPeerConnection;
  channel: RTCDataChannel;
  data: any;
}

export interface VerifyOptions {
  id: string;
  credentials: any;
}

export interface SenderConfig {
  driver: Driver;
  iceServers?: RTCIceServer[];
  verify?: (options: VerifyOptions) => boolean;
  connectionTimeout?: number;
  audioBitrate?: number;
  videoBitrate?: number;
}

export interface SenderStartOptions {
  room: string;
  stream?: MediaStream;
  metadata?: any;
  channels?: { [label: string]: object };
}

export interface SenderEventMap {
  'connect': ConnectEvent;
  'dispose': DisposeEvent;
  'error': ErrorEvent;
  'channel:open': ChannelOpenEvent;
  'channel:close': ChannelCloseEvent;
  'channel:error': ChannelErrorEvent;
  'channel:message': ChannelMessageEvent;
}

export declare class Sender extends EventTarget {
  constructor(config: SenderConfig);

  start(options?: SenderStartOptions): void;
  stop(): void;

  addEventListener<K extends keyof SenderEventMap>(
    type: K,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | AddEventListenerOptions
  ): void;

  removeEventListener<K extends keyof SenderEventMap>(
    type: K,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | EventListenerOptions
  ): void;
}

export interface ReceiverConfig {
  driver: Driver;
  iceServers?: RTCIceServer[];
  connectionTimeout?: number;
  pingInterval?: number;
  pingAttempts?: number;
}

export interface ReceiverStartOptions {
  room: string;
  credentials?: any;
}

export interface ReceiverEventMap {
  'connect': ConnectEvent;
  'stream': StreamEvent;
  'dispose': DisposeEvent;
  'error': ErrorEvent;
  'channel:open': ChannelOpenEvent;
  'channel:close': ChannelCloseEvent;
  'channel:error': ChannelErrorEvent;
  'channel:message': ChannelMessageEvent;
}

export declare class Receiver extends EventTarget {
  constructor(config: ReceiverConfig);

  start(options?: ReceiverStartOptions): void;
  stop(): void;

  addEventListener<K extends keyof ReceiverEventMap>(
    type: K,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | AddEventListenerOptions
  ): void;

  removeEventListener<K extends keyof ReceiverEventMap>(
    type: K,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | EventListenerOptions
  ): void;
}
