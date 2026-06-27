/**
 * A counter-based lock that tracks active operations and allows waiting until
 * all operations have completed without polling.
 *
 * Each call to `acquire()` increments the internal counter, and each call to
 * `release()` decrements it. Callers can await `waitForIdle()` which resolves
 * exactly when the counter drops back to zero, or rejects on timeout.
 *
 * @example
 * ```ts
 * const lock = new IdleLock();
 *
 * lock.acquire();
 * try {
 *   await doSomeWork();
 * } finally {
 *   lock.release(); // resolves any pending waitForIdle calls
 * }
 *
 * // Elsewhere, wait for all work to finish:
 * await lock.waitForIdle(5000); // throws if still busy after 5 seconds
 * ```
 */
export class IdleLock {
  #count = 0;
  #pendingWaiters: Array<() => void> = [];
  #disposed = false;

  /**
   * Gets the current number of active (acquired) operations.
   */
  get count(): number {
    return this.#count;
  }

  /**
   * Checks if no operations are currently in progress.
   */
  get isIdle(): boolean {
    return this.#count === 0;
  }

  /**
   * Acquires the lock, indicating a new operation has started.
   *
   * Has no effect if the lock has been disposed.
   */
  acquire(): void {
    if (this.#disposed) return;
    this.#count++;
  }

  /**
   * Releases the lock, indicating an operation has completed.
   *
   * If the counter drops to zero after this release, all pending waiters
   * from `waitForIdle()` are resolved. Has no effect if the lock is disposed
   * or the counter is already at zero.
   */
  release(): void {
    if (this.#disposed || this.#count <= 0) return;

    this.#count--;

    if (this.#count === 0 && this.#pendingWaiters.length > 0) {
      const waiters = this.#pendingWaiters;
      this.#pendingWaiters = [];
      for (const resolve of waiters) {
        resolve();
      }
    }
  }

  /**
   * Waits until no operations are in progress.
   *
   * Resolves immediately if the lock is already idle or has been disposed.
   * Rejects with a timeout error if `timeoutMs` is provided and the lock does
   * not become idle within that duration.
   *
   * @param timeoutMs Optional maximum time to wait in milliseconds. Omit for an indefinite wait.
   */
  waitForIdle(timeoutMs?: number): Promise<void> {
    const promise = new Promise<void>((resolve) => {
      if (this.#count === 0 || this.#disposed) {
        resolve();
        return;
      }
      this.#pendingWaiters.push(resolve);
    });

    if (timeoutMs === undefined) {
      return promise;
    }

    const timeout = new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error("Lock timeout")), timeoutMs),
    );

    return Promise.race([promise, timeout]);
  }

  /**
   * Disposes the lock, resolving all pending waiters immediately.
   *
   * After disposal, `acquire()` and `release()` become no-ops and
   * `waitForIdle()` resolves instantly regardless of counter state.
   */
  dispose(): void {
    this.#disposed = true;
    if (this.#pendingWaiters.length > 0) {
      const waiters = this.#pendingWaiters;
      this.#pendingWaiters = [];
      for (const resolve of waiters) {
        resolve();
      }
    }
  }
}
