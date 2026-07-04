const colors = {
    reset: "\x1b[0m",
    info: "\x1b[32m", // Green
    warn: "\x1b[33m", // Yellow
    error: "\x1b[31m", // Red
    debug: "\x1b[36m", // Cyan
};

function getFormattedTime(): string {
    return new Date().toISOString();
}

function print(level: keyof typeof colors, method: 'log' | 'warn' | 'error', ...messages: any[]) {
    const time = getFormattedTime();
    const colorCode = colors[level];
    const resetCode = colors.reset;
    const levelStr = level.toUpperCase();

    const prefix = `[${colorCode}${levelStr}${resetCode}] [${time}]`;
    console[method](prefix, ...messages);
}

export const logger = {
    info: (...args: any[]) => print('info', 'log', ...args),
    warn: (...args: any[]) => print('warn', 'warn', ...args),
    error: (...args: any[]) => print('error', 'error', ...args),
    debug: (...args: any[]) => print('debug', 'log', ...args)
};