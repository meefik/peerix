/**
 * A utility class for managing timeouts, allowing for easy starting and clearing of timeouts.
 */
export class Timeout {
  #timer?: ReturnType<typeof setTimeout>;
  #callback: () => void;
  #delay: number;

  constructor(callback: () => void, delay: number) {
    this.#callback = callback;
    this.#delay = delay;
    this.start();
  }

  start(delay?: number) {
    this.clear();
    this.#timer = setTimeout(this.#callback, delay ?? this.#delay);
  }

  clear() {
    if (this.#timer) {
      clearTimeout(this.#timer);
      this.#timer = undefined;
    }
  }
}
