/**
 * A utility class for managing timeouts, allowing easy starting and clearing.
 */
export class Timeout {
  #timer?: ReturnType<typeof setTimeout>;
  #callback: () => void;
  #delay: number;

  /**
   * Creates a new Timeout instance.
   *
   * @param callback The function to be called when the timeout expires.
   * @param delay The delay in milliseconds before the timeout expires.
   */
  constructor(callback: () => void, delay: number) {
    this.#callback = callback;
    this.#delay = delay;
  }

  /**
   * Starts the timeout.
   *
   * @param delay Optional delay in milliseconds before the timeout expires. If not provided, the initial delay is used.
   */
  start(delay?: number): void {
    clearTimeout(this.#timer);
    this.#timer = setTimeout(this.#callback, delay ?? this.#delay);
  }

  /**
   * Clears the timeout if it is currently active.
   */
  stop(): void {
    if (this.#timer) {
      clearTimeout(this.#timer);
      this.#timer = undefined;
    }
  }
}
