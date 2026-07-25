/**
 * Normalizes options into a plain object.
 *
 * If the input is an object, it is shallow-copied. Otherwise, the optional
 * parser function is invoked to derive an object from the raw value.
 *
 * @param options The options to parse.
 * @param parser A transformer invoked when the input is not an object.
 * @returns The normalized options as a plain object.
 */
export function parseOptions<T>(
  options: unknown | T | undefined,
  parser?: (options: unknown | undefined) => Partial<T>,
): Partial<T> {
  return typeof options === "object" && options !== null
    ? { ...options }
    : options !== undefined && typeof parser === "function"
      ? { ...parser(options) }
      : {};
}
