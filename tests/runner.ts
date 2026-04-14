import { Peer } from '../src/index.js';
import type { PeerEvents } from '../src/index.js';

type PeerEventName = keyof PeerEvents;

type PeerCall = 'open' | 'close' | 'send' | 'join' | 'publish' | 'unpublish' | 'leave';

type CallStep = {
  peer: string;
  call: PeerCall;
  args?: readonly any[];
};

type WaitStep = {
  peer: string;
  wait: PeerEventName;
  where?: Record<string, any>;
  count?: number;
  timeout?: number;
};

type Step = CallStep | WaitStep;

type Scenario = {
  defaults: { timeout: number; };
  peers: readonly { id: string; }[];
  steps: readonly Step[];
};

type SyntheticStreamParams = {
  width?: number;
  height?: number;
  audio?: boolean;
  video?: boolean;
  fps?: number;
};

export class TestRunner {
  private peers: Record<string, Peer> = {};
  private eventStore: Record<string, Map<PeerEventName, any[]>> = {};
  private cursors: Record<string, Map<PeerEventName, number>> = {};
  private waiters: Record<string, Map<PeerEventName, (() => void)[]>> = {};

  constructor(options?: { debug?: string; }) {
    if (options?.debug) localStorage.debug = options.debug;
  }

  private isCallStep(step: Step): step is CallStep {
    return 'call' in step;
  }

  private isWaitStep(step: Step): step is WaitStep {
    return 'wait' in step;
  }

  private isRecord(value: unknown): value is Record<string, any> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
  }

  private matchesExpected(actual: any, expected: any): boolean {
    if (Array.isArray(expected)) {
      if (!Array.isArray(actual) || actual.length !== expected.length) return false;
      return expected.every((value, index) => this.matchesExpected(actual[index], value));
    }

    if (this.isRecord(expected)) {
      if (!this.isRecord(actual)) return false;
      for (const [key, value] of Object.entries(expected)) {
        const nestedActual = actual?.[key];
        if (!this.matchesExpected(nestedActual, value)) return false;
      }
      return true;
    }

    return actual === expected;
  }

  private createSyntheticMediaStream({ width = 640, height = 360, audio = true, video = true, fps = 15 }: SyntheticStreamParams = {}): MediaStream {
    const tracks: MediaStreamTrack[] = [];
    let draw = () => { };

    if (video) {
      const canvas = document.createElement('canvas');
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext('2d');
      if (!ctx) {
        throw new Error('Failed to create canvas context');
      }

      draw = () => {
        ctx.fillStyle = 'black';
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.fillStyle = 'lime';
        ctx.font = `${Math.min(canvas.width, canvas.height) * 0.2}px monospace`;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(`${new Date().toLocaleTimeString()}`, canvas.width / 2, canvas.height / 2);
        if (stream?.active) setTimeout(draw, ~~(1000 / fps));
      };

      const videoStream = canvas.captureStream(fps);
      tracks.push(...videoStream.getVideoTracks());
    }

    if (audio) {
      const audioCtx = new window.AudioContext();
      const oscillator = audioCtx.createOscillator();
      const dst = audioCtx.createMediaStreamDestination();

      oscillator.type = 'sine';
      oscillator.frequency.setValueAtTime(440, audioCtx.currentTime);
      oscillator.connect(dst);
      oscillator.start();

      tracks.push(...dst.stream.getAudioTracks());
    }

    const stream = new MediaStream(tracks);
    draw();
    return stream;
  }

  private setupEvents(events: PeerEventName[]): void {
    for (const peerId of Object.keys(this.peers)) {
      this.eventStore[peerId] = new Map();
      this.cursors[peerId] = new Map();
      this.waiters[peerId] = new Map();
      for (const event of events) {
        this.eventStore[peerId].set(event, []);
        this.cursors[peerId].set(event, 0);
        this.waiters[peerId].set(event, []);
        this.peers[peerId].on(event, (payload: any) => {
          this.eventStore[peerId].get(event)?.push(payload);
          const queue = this.waiters[peerId].get(event) ?? [];
          for (const wake of queue) wake();
        });
      }
    }
  }

  private normalizeCallArgs(step: CallStep): any[] {
    const args = [...(step.args ?? [])];

    if (step.call === 'publish') {
      const [options] = args;
      if (options.stream instanceof MediaStream === false) {
        options.stream = this.createSyntheticMediaStream(options.stream);
        options.managed = true;
      }
    }

    return args;
  }

  private waitForEvent(
    peerId: string,
    event: PeerEventName,
    where: Record<string, any> | undefined,
    count: number,
    timeout: number,
  ): Promise<any[]> {
    const started = Date.now();
    return new Promise<any[]>((resolve, reject) => {
      const scan = () => {
        const from = this.cursors[peerId].get(event) ?? 0;
        const queue = this.eventStore[peerId].get(event) ?? [];
        const tail = queue.slice(from);
        const matched = tail.filter((payload) => this.matchesExpected(payload, where));

        if (matched.length >= count) {
          if (where === undefined) {
            this.cursors[peerId].set(event, from + count);
          }
          else {
            // Remove only matched events so out-of-order payloads stay available for later waits.
            let remaining = count;
            for (let index = from; index < queue.length && remaining > 0; index += 1) {
              if (this.matchesExpected(queue[index], where)) {
                queue.splice(index, 1);
                index -= 1;
                remaining -= 1;
              }
            }
            const nextCursor = Math.min(this.cursors[peerId].get(event) ?? 0, queue.length);
            this.cursors[peerId].set(event, nextCursor);
          }

          cleanup();
          resolve(matched.slice(0, count));
        } else if (Date.now() - started > timeout) {
          cleanup();
          reject(new Error(`Timeout waiting for "${event}" on peer "${peerId}"`));
        }
      };

      const wake = () => scan();
      const timer = setInterval(scan, 25);
      const cleanup = () => {
        clearInterval(timer);
        const list = this.waiters[peerId].get(event) ?? [];
        const idx = list.indexOf(wake);
        if (idx >= 0) list.splice(idx, 1);
      };

      this.waiters[peerId].get(event)?.push(wake);
      scan();
    });
  }

  async run(scenario: Scenario) {
    for (const options of scenario.peers) {
      const { id } = options;
      this.peers[id] = new Peer(options);
    }

    const events = [
      ...new Set(
        scenario.steps
          .filter((step): step is WaitStep => !this.isCallStep(step))
          .map((step) => step.wait),
      ),
    ];

    this.setupEvents(events);

    for (const step of scenario.steps) {
      if (this.isCallStep(step)) {
        const peer = this.peers[step.peer];
        const call = (peer as any)[step.call];
        const args = this.normalizeCallArgs(step);
        await call.apply(peer, args);
      }
      else if (this.isWaitStep(step)) {
        await this.waitForEvent(
          step.peer,
          step.wait,
          step.where,
          step.count ?? 1,
          step.timeout ?? scenario.defaults.timeout,
        );
      }
    }
  }
}
