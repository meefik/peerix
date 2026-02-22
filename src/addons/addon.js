import EventEmitter from '../utils/emitter.js';

export class Addon {
  constructor() {
    this.emitter = new EventEmitter(this);
  }

  // async attach(peer) {
  //   console.log('Addon: attached to peer', peer);
  // }

  // async detach(peer) {
  //   console.log('Addon: detached from peer', peer);
  // }

  // async offer(peer, conn) {
  //   console.log('Addon: offer');
  // }

  // async answer(peer, conn) {
  //   console.log('Addon: answer');
  // }

  // async paired(peer, conn) {
  //   console.log('Addon: pair');
  // }

  // async dispose(peer, conn) {
  //   console.log('Addon: dispose');
  // }

  on(event, handler) {
    this.emitter.on(event, handler);
  }

  once(event, handler) {
    this.emitter.once(event, handler);
  }

  off(event, handler) {
    this.emitter.off(event, handler);
  }

  emit(event, ...args) {
    this.emitter.emit(event, ...args);
  }
}
