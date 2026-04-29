import { Driver } from './driver.js';

/**
 * WebSocket-based signaling driver with auto-reconnection and ping/pong support.
 * It maintains a map of namespaces to sets of handlers, and automatically
 * re-subscribes to namespaces after reconnection. It also queues messages when
 * the socket is not opened, and flushes them upon connection.
 * 
 * @group Drivers
 * @example
 * ```javascript
 * const driver = new WebSocketDriver({ url: 'wss://localhost:8443/ws' });
 * ```
 */
export class WebSocketDriver extends Driver {
  private _url: string;
  private _protocols?: string | string[];
  private _reconnection: boolean;
  private _reconnectionDelay: number;
  private _reconnectionDelayMax: number;
  private _reconnectionAttempts: number;
  private _randomizationFactor: number;
  private _pingInterval: number;
  private _pingTimeout: number;
  private _queueLimit: number;
  private _queue: any[];
  private _attempts: number;
  private _ws?: WebSocket;
  private _pingTimer?: any;
  private _reconnectTimer?: any;
  private _handlers: Map<string, Set<(data: any) => void>>;

  /**
   * Indicates whether the WebSocket connection is currently open.
   */
  get opened() {
    return this._ws?.readyState === WebSocket.OPEN;
  }

  /**
   * Creates a new WebSocketDriver instance with the specified options.
   * 
   * @param options Configuration options for the driver.
   * @param options.url The WebSocket server URL (default: '/ws').
   * @param options.protocols Optional subprotocols for the WebSocket connection.
   * @param options.reconnection Whether to enable auto-reconnection (default: true).
   * @param options.reconnectionDelay Initial delay for reconnection attempts in ms (default: 5000).
   * @param options.reconnectionDelayMax Maximum delay for reconnection attempts in ms (default: 60000).
   * @param options.reconnectionAttempts Maximum number of reconnection attempts (default: Infinity).
   * @param options.randomizationFactor Randomization factor for reconnection delay (default: 0.5).
   * @param options.pingInterval Interval for sending ping messages in ms (default: 30000).
   * @param options.pingTimeout Timeout for receiving pong responses in ms (default: 10000).
   * @param options.queueLimit Maximum number of queued messages when socket is not opened (default: 100).
   */
  constructor(options?: {
    url?: string;
    protocols?: string | string[],
    reconnection?: boolean,
    reconnectionDelay?: number,
    reconnectionDelayMax?: number,
    reconnectionAttempts?: number,
    randomizationFactor?: number,
    pingInterval?: number,
    pingTimeout?: number,
    queueLimit?: number,
  }) {
    super();
    const {
      url = '/ws',
      protocols,
      reconnection = true,
      reconnectionDelay = 5000,
      reconnectionDelayMax = 60000,
      reconnectionAttempts = Infinity,
      randomizationFactor = 0.5,
      pingInterval = 30000,
      pingTimeout = 10000,
      queueLimit = 100, // max queued messages when socket is not opened
    } = options || {};
    this._url = url;
    this._protocols = protocols;
    this._reconnection = reconnection;
    this._reconnectionDelay = reconnectionDelay;
    this._reconnectionDelayMax = reconnectionDelayMax;
    this._reconnectionAttempts = reconnectionAttempts;
    this._randomizationFactor = randomizationFactor;
    this._pingInterval = pingInterval;
    this._pingTimeout = pingTimeout;
    this._queueLimit = queueLimit;
    this._queue = [];
    this._attempts = 0;
    this._handlers = new Map();
  }

  /**
   * Opens a WebSocket connection to the server.
   */
  open() {
    if (this._ws) this.close();

    const ws = new WebSocket(this._url, this._protocols);
    this._ws = ws;

    let pingTime = Date.now();
    let pongTime = 0;

    const scheduleReconnect = () => {
      if (!this._reconnection || this._attempts >= this._reconnectionAttempts) return;

      const delay = Math.min(
        this._reconnectionDelay + 1000 * this._attempts * (1 + Math.random() * this._randomizationFactor),
        this._reconnectionDelayMax,
      );

      this._attempts++;
      this._reconnectTimer = setTimeout(() => this.open(), delay);
    };

    const dispose = (error?: any) => {
      if (this._ws !== ws) return;

      console.error('WebSocket error:', error);

      clearTimeout(this._reconnectTimer);
      clearInterval(this._pingTimer);

      ws.onclose = null;
      ws.onopen = null;
      ws.onmessage = null;
      ws.onerror = null;

      scheduleReconnect();
    };

    ws.onmessage = (e) => {
      if (this._ws !== ws) return;

      const message = e.data;
      if (message === 'pong') {
        pongTime = Date.now();
        return;
      }

      const [ns, data] = JSON.parse(message);
      if (!this._handlers.has(ns)) return;

      for (const handler of this._handlers.get(ns) || []) {
        setTimeout(() => handler(data), 0);
      }
    };

    ws.onopen = () => {
      if (this._ws !== ws) return;

      this._attempts = 0;

      // restore room membership after failures
      if (this._handlers.size) {
        this._ws.send(JSON.stringify(['>', Array.from(this._handlers.keys())]));
      }

      // flush queued messages
      while (this._queue.length) {
        const [ns, data] = this._queue.shift();
        this._ws.send(JSON.stringify([ns, data]));
      }

      clearInterval(this._pingTimer);
      this._pingTimer = setInterval(() => {
        const now = Date.now();
        if (this._ws && this.opened && now - pingTime > this._pingInterval) {
          pingTime = now;
          this._ws.send('ping');
        }
        if (pongTime <= pingTime && now - pingTime > this._pingTimeout) {
          dispose(new Error('Ping timeout'));
        }
      }, 1000);
    };

    ws.onclose = () => dispose();

    ws.onerror = (error) => dispose(error);
  }

  /**
   * Closes the WebSocket connection and clears all handlers and queued messages.
   */
  close() {
    if (this._ws) {
      this._ws.close();
      delete this._ws;
    }
    this._handlers.clear();
  }

  async subscribe(namespace: string[], handler: (data: any) => void) {
    const ns = namespace.join(':');
    let handlers = this._handlers.get(ns);
    const isNew = !handlers;
    if (!handlers) {
      handlers = new Set();
      this._handlers.set(ns, handlers);
    }
    handlers.add(handler);
    if (isNew && this._ws && this.opened) {
      this._ws.send(JSON.stringify(['>', ns]));
    }
  }

  async unsubscribe(namespace: string[], handler: (data: any) => void) {
    const ns = namespace.join(':');
    const handlers = this._handlers.get(ns);
    if (handlers) {
      if (handler) handlers.delete(handler);
      else handlers.clear();
      if (!handlers.size) {
        this._handlers.delete(ns);
        if (this._ws && this.opened) {
          this._ws.send(JSON.stringify(['<', ns]));
        }
      }
    }
  }

  async dispatch(namespace: string[], message: any) {
    const ns = namespace.join(':');
    if (this._ws && this.opened) {
      this._ws.send(JSON.stringify([ns, message]));
    }
    else {
      this._queue.push([ns, message]);
      if (this._queue.length > this._queueLimit) {
        this._queue.shift();
      }
    }
  }
}
