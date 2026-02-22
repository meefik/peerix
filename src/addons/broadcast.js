import { Addon } from './addon.js';

export class Broadcast extends Addon {
  streams = new Set();

  // constructor() {
  //   super();
  //   this._offer = (self, conn) => {
  //     for (const stream of this.streams) {
  //       stream.getTracks().forEach(track =>
  //         conn.peer.addTrack(track, stream),
  //       );
  //     }
  //   };
  // }

  _offer(self, conn) {
    for (const stream of this.streams) {
      stream.getTracks().forEach(track =>
        conn.peer.addTrack(track, stream),
      );
    }
  }

  events() {
    return {
      offer(self, conn) {
        for (const stream of this.streams) {
          stream.getTracks().forEach(track =>
            conn.peer.addTrack(track, stream),
          );
        }
      },
    };
  }

  attach(self) {
    self.on('offer', this._offer);

    self.forEach((conn) => {
      const senders = conn.peer.getSenders();
      for (const stream of this.streams) {
        stream.getTracks().forEach((track) => {
          const sender = senders.find((sender) => {
            return sender.track && sender.track.kind === track.kind
              && sender.track.readyState === 'ended';
          });
          if (sender) sender.replaceTrack(track);
          else conn.peer.addTrack(track, stream);
        });
      }
    });
  }

  detach(self) {
    self.off('offer', this._offer);

    self.forEach((conn) => {
      const senders = conn.peer.getSenders();
      for (const stream of this.streams) {
        stream.getTracks().forEach((track) => {
          const sender = senders.find(sender => sender.track === track);
          if (sender) conn.peer.removeTrack(sender);
        });
      }
    });
  }

  publish(stream) {
    this.streams.add(stream);
  }

  unpublish(stream) {
    this.streams.delete(stream);
  }
}
