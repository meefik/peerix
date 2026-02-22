import { Addon } from './addon.js';

export class Channel extends Addon {
  constructor(label, options) {
    super();
    this.label = label;
    this.options = options || {};
  }

  attach(self) {
    self.forEach((conn) => {
      const dataChannel = conn.peer.createDataChannel(this.label, this.options);
      conn.channels.set(this.label, dataChannel);
    });
  }

  detach(self) {
    self.forEach((conn) => {
      const senders = conn.peer.getSenders();
      this.stream.getTracks().forEach((track) => {
        const sender = senders.find(sender => sender.track === track);
        if (sender) conn.peer.removeTrack(sender);
      });
    });
  }
}
