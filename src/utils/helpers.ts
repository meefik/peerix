/**
 * Normalizes options into a plain object.
 *
 * @param options The options to parse. If an object, it is shallow-copied; otherwise the parser is applied.
 * @param parser A transformer invoked when the input is not an object.
 * @returns The normalized options as a plain object.
 */
export function parseOptions<T>(
  options: unknown | T | undefined,
  parser?: (options: unknown | undefined) => Partial<T>,
): Partial<T> {
  return typeof options === "object" && options !== null
    ? { ...options }
    : typeof options !== "undefined" && typeof parser === "function"
      ? { ...parser(options) }
      : {};
}
