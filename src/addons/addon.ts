import EventEmitter from '../utils/emitter.js';
import { Peer } from '../peer.js';

export class Addon {
  private _emitter: EventEmitter;

  constructor() {
    this._emitter = new EventEmitter(this);
  }

  async attach(peer: Peer) {
    // stub
  }

  async detach(peer: Peer) {
    // stub
  }

  on(event: string | string[], handler: (...args: any[]) => void) {
    this._emitter.on(event, handler);
  }

  once(event: string | string[], handler: (...args: any[]) => void) {
    this._emitter.once(event, handler);
  }

  off(event: string | string[], handler?: (...args: any[]) => void) {
    this._emitter.off(event, handler);
  }

  emit(event: string | string[], ...args: any[]) {
    this._emitter.emit(event, ...args);
  }
}
