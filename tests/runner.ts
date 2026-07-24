import type { RoomEvents, RoomOptions } from "../src/index.js";
import { Room } from "../src/index.js";

type EventName = keyof RoomEvents;

type RoomCall =
  | "open"
  | "close"
  | "send"
  | "join"
  | "share"
  | "unshare"
  | "leave";

type CallStep = {
  room: string;
  call: RoomCall;
  args?: readonly any[];
};

type WaitStep = {
  room: string;
  wait: EventName;
  where?: Record<string, any>;
  count?: number;
  timeout?: number;
};

type Step = CallStep | WaitStep;

type Scenario = {
  id: string;
  title?: string;
  defaults?: Record<string, any>;
  rooms?: Record<string, RoomOptions>;
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
  private rooms: Record<string, Room> = {};
  private eventStore: Record<string, Map<EventName, any[]>> = {};
  private cursors: Record<string, Map<EventName, number>> = {};
  private waiters: Record<string, Map<EventName, (() => void)[]>> = {};

  constructor(options?: { debug?: string }) {
    if (options?.debug) localStorage.debug = options.debug;
  }

  private isCallStep(step: Step): step is CallStep {
    return "call" in step;
  }

  private isWaitStep(step: Step): step is WaitStep {
    return "wait" in step;
  }

  private isRecord(value: unknown): value is Record<string, any> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }

  private async matchesExpected(actual: any, expected: any): Promise<boolean> {
    if (Array.isArray(expected)) {
      if (!Array.isArray(actual) || actual.length !== expected.length)
        return false;
      for (let i = 0; i < expected.length; i++) {
        if (!(await this.matchesExpected(actual[i], expected[i]))) return false;
      }
      return true;
    }

    if (this.isRecord(expected)) {
      if (!this.isRecord(actual)) return false;
      for (const [key, value] of Object.entries(expected)) {
        let nestedActual = actual?.[key];
        // Resolve PromiseLike data fields (e.g., channel:message data)
        if (
          key === "data" &&
          nestedActual !== null &&
          typeof nestedActual === "object" &&
          "then" in nestedActual
        ) {
          nestedActual = await nestedActual;
        }
        if (!(await this.matchesExpected(nestedActual, value))) return false;
      }
      return true;
    }

    return actual === expected;
  }

  private createSyntheticMediaStream({
    width = 640,
    height = 360,
    audio = true,
    video = true,
    fps = 15,
  }: SyntheticStreamParams = {}): MediaStream {
    const tracks: MediaStreamTrack[] = [];
    let draw = () => {};

    if (video) {
      const canvas = document.createElement("canvas");
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext("2d");
      if (!ctx) {
        throw new Error("Failed to create canvas context");
      }

      draw = () => {
        ctx.fillStyle = "black";
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.fillStyle = "lime";
        ctx.font = `${Math.min(canvas.width, canvas.height) * 0.2}px monospace`;
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(
          `${new Date().toLocaleTimeString()}`,
          canvas.width / 2,
          canvas.height / 2,
        );
        if (stream?.active) setTimeout(draw, ~~(1000 / fps));
      };

      const videoStream = canvas.captureStream(fps);
      tracks.push(...videoStream.getVideoTracks());
    }

    if (audio) {
      const audioCtx = new window.AudioContext();
      const oscillator = audioCtx.createOscillator();
      const dst = audioCtx.createMediaStreamDestination();

      oscillator.type = "sine";
      oscillator.frequency.setValueAtTime(440, audioCtx.currentTime);
      oscillator.connect(dst);
      oscillator.start();

      tracks.push(...dst.stream.getAudioTracks());
    }

    const stream = new MediaStream(tracks);
    draw();
    return stream;
  }

  private setupEvents(events: EventName[]): void {
    for (const room of Object.keys(this.rooms)) {
      this.eventStore[room] = new Map();
      this.cursors[room] = new Map();
      this.waiters[room] = new Map();
      for (const event of events) {
        this.eventStore[room].set(event, []);
        this.cursors[room].set(event, 0);
        this.waiters[room].set(event, []);
        this.rooms[room].on(event, (payload: any) => {
          this.eventStore[room].get(event)?.push(payload);
          const queue = this.waiters[room].get(event) ?? [];
          for (const wake of queue) wake();
        });
      }
    }
  }

  private normalizeCallArgs(step: CallStep): any[] {
    const args = [...(step.args ?? [])];

    if (step.call === "share") {
      const [options] = args;
      if (options.stream instanceof MediaStream === false) {
        options.stream = this.createSyntheticMediaStream(options.stream);
      }
    }

    return args;
  }

  private waitForEvent(
    room: string,
    event: EventName,
    where: Record<string, any> | undefined,
    count: number,
    timeout: number,
  ): Promise<any[]> {
    const started = Date.now();
    return new Promise<any[]>((resolve, reject) => {
      const scan = async () => {
        const from = this.cursors[room].get(event) ?? 0;
        const queue = this.eventStore[room].get(event) ?? [];
        const tail = queue.slice(from);
        const matched: any[] = [];
        for (const payload of tail) {
          if (await this.matchesExpected(payload, where)) {
            matched.push(payload);
          }
        }

        if (matched.length >= count) {
          if (where === undefined) {
            this.cursors[room].set(event, from + count);
          } else {
            // Remove only matched events so out-of-order payloads stay available for later waits.
            let remaining = count;
            for (
              let index = from;
              index < queue.length && remaining > 0;
              index += 1
            ) {
              if (await this.matchesExpected(queue[index], where)) {
                queue.splice(index, 1);
                index -= 1;
                remaining -= 1;
              }
            }
            const nextCursor = Math.min(
              this.cursors[room].get(event) ?? 0,
              queue.length,
            );
            this.cursors[room].set(event, nextCursor);
          }

          cleanup();
          resolve(matched.slice(0, count));
        } else if (Date.now() - started > timeout) {
          cleanup();
          reject(
            new Error(`Timeout waiting for "${event}" on room "${room}"`),
          );
        }
      };

      const wake = () => scan();
      const timer = setInterval(() => scan(), 25);
      const cleanup = () => {
        clearInterval(timer);
        const list = this.waiters[room].get(event) ?? [];
        const idx = list.indexOf(wake);
        if (idx >= 0) list.splice(idx, 1);
      };

      this.waiters[room].get(event)?.push(wake);
      scan();
    });
  }

  async run(scenario: Scenario) {
    for (const room in scenario.rooms) {
      const options = scenario.rooms[room];
      this.rooms[room] = new Room(options);
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
        const room = this.rooms[step.room];
        const call = (room as any)[step.call];
        const args = this.normalizeCallArgs(step);
        await call.apply(room, args);
      } else if (this.isWaitStep(step)) {
        await this.waitForEvent(
          step.room,
          step.wait,
          step.where,
          step.count ?? 1,
          step.timeout ?? scenario.defaults?.timeout,
        );
      }
    }
  }
}
