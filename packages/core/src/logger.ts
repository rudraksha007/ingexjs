/**
 * ANSI color codes for terminal output formatting.
 * - reset: Resets the terminal color
 * - info: Green color for info messages
 * - warn: Yellow color for warn messages
 * - error: Red color for error messages
 * - debug: Cyan color for debug messages
 */
const colors = {
    reset: "\x1b[0m",
    info: "\x1b[32m", // Green
    warn: "\x1b[33m", // Yellow
    error: "\x1b[31m", // Red
    debug: "\x1b[36m", // Cyan
};

/**
 * Gets the current time formatted as an ISO string.
 * @returns The formatted timestamp.
 */
function getFormattedTime(): string {
    return new Date().toISOString();
}

/**
 * Prints a log message to the console with color formatting and a timestamp.
 * 
 * @param level - The log level determining the color prefix.
 * @param method - The console method to use.
 * @param messages - The messages or objects to log.
 */
function print(level: keyof typeof colors, method: 'log' | 'warn' | 'error', ...messages: any[]) {
    const time = getFormattedTime();
    const colorCode = colors[level];
    const resetCode = colors.reset;
    const levelStr = level.toUpperCase();

    const prefix = `[${colorCode}${levelStr}${resetCode}] [${time}]`;
    console[method](prefix, ...messages);
}

/**
 * Simple logger utility for the ingestor core package.
 * Provides info, warn, error, and debug logging methods.
 */
export const logger = {
    info: (...args: any[]) => print('info', 'log', ...args),
    warn: (...args: any[]) => print('warn', 'warn', ...args),
    error: (...args: any[]) => print('error', 'error', ...args),
    debug: (...args: any[]) => print('debug', 'log', ...args)
};