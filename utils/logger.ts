// utils/logger.ts
type LogLevel = 'debug' | 'info' | 'warn' | 'error'

interface LoggerOptions {
  level?: LogLevel
  timestamp?: boolean
  metadata?: Record<string, any>
}

// Store the original console methods to prevent recursive calls
const originalConsoleMethods = {
  error: console.error,
  log: console.log,
  warn: console.warn,
  debug: console.debug
}

export const Logger = {
  info: (message: string, ...args: any[]) => {
    if (process.env.NEXT_PUBLIC_DEBUG === 'true' || process.env.NODE_ENV !== 'production') {
      const timestamp = new Date().toISOString()
      originalConsoleMethods.log(`[INFO] [${timestamp}] ${message}`, ...args)
    }
  },

  error: (message: string, error?: any) => {
    const timestamp = new Date().toISOString()
    
    // Use original console.error to prevent recursive logging
/**
 * Logs a warning message to the console along with optional additional arguments.
 * The message is prefixed with a timestamp and labeled as a warning.
 * Logging occurs only when the environment is not production or when debug mode is enabled.
 *
 * @param message - The main warning message to log.
 * @param args - Additional optional arguments to log.
 */

    try {
      originalConsoleMethods.error(`[ERROR] [${timestamp}] ${message}`, error)
      
      // Additional error tracking
      if (error instanceof Error) {
        originalConsoleMethods.error('Stack trace:', error.stack)
      }
    } catch (logError) {
      // Fallback logging if something goes wrong
      originalConsoleMethods.error('Error during logging:', logError)
    }
  },

  warn: (message: string, ...args: any[]) => {
    if (process.env.NODE_ENV !== 'production' || process.env.NEXT_PUBLIC_DEBUG === 'true') {
      const timestamp = new Date().toISOString()
      originalConsoleMethods.warn(`[WARN] [${timestamp}] ${message}`, ...args)
    }
  },

  debug: (message: string, ...args: any[]) => {
    if (process.env.NODE_ENV === 'development' || process.env.NEXT_PUBLIC_DEBUG === 'true') {
      const timestamp = new Date().toISOString()
      originalConsoleMethods.debug(`[DEBUG] [${timestamp}] ${message}`, ...args)
    }
  },

  log: (level: LogLevel, message: string, options?: LoggerOptions, ...args: any[]) => {
    const shouldLog =
      process.env.NODE_ENV === 'development' ||
      process.env.NEXT_PUBLIC_DEBUG === 'true' ||
      level === 'error'
    
    if (!shouldLog) return

    const timestamp = options?.timestamp ? new Date().toISOString() : undefined
    const metadata = options?.metadata ? JSON.stringify(options.metadata) : ''

    const logMessage = [
      `[${level.toUpperCase()}]`,
      timestamp ? `[${timestamp}]` : '',
      metadata ? `[${metadata}]` : '',
      message
    ].filter(Boolean).join(' ')

    try {
      switch (level) {
        case 'debug':
          originalConsoleMethods.debug(logMessage, ...args)
          break
        case 'info':
          originalConsoleMethods.log(logMessage, ...args)
          break
        case 'warn':
          originalConsoleMethods.warn(logMessage, ...args)
          break
        case 'error':
          originalConsoleMethods.error(logMessage, ...args)
          // Log stack trace if last argument is an Error
          const lastArg = args[args.length - 1]
          if (lastArg instanceof Error) {
            originalConsoleMethods.error('Stack trace:', lastArg.stack)
          }
          break
      }
    } catch (logError) {
      // Fallback logging if something goes wrong
      originalConsoleMethods.error('Error during logging:', logError)
    }
  }
}

export default Logger