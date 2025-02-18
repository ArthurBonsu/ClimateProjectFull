import React, { PropsWithChildren, useEffect, useState } from 'react';
import { Logger } from '../utils/logger';

const GlobalErrorHandler: React.FC<PropsWithChildren<{}>> = ({ children }) => {
  const [isClient, setIsClient] = useState(false);

  useEffect(() => {
    // Mark as client-side
    setIsClient(true);

    // Store the original console methods
    const originalConsoleError = console.error;
    const originalConsoleLog = console.log;

    // Global error logging
    const handleError = (event: ErrorEvent) => {
      try {
        Logger.error('Uncaught Client-Side Error', {
          message: event.error?.message,
          stack: event.error?.stack,
          filename: event.filename,
          lineno: event.lineno,
          colno: event.colno
        });
      } catch {
        // Fallback to original console if Logger fails
        originalConsoleError('Uncaught Client-Side Error', {
          message: event.error?.message,
          stack: event.error?.stack
        });
      }
    };

    // Unhandled Promise Rejection Logging
    const handleUnhandledRejection = (event: PromiseRejectionEvent) => {
      try {
        Logger.error('Unhandled Promise Rejection', {
          reason: event.reason,
          promise: event.promise
        });
      } catch {
        // Fallback to original console if Logger fails
        originalConsoleError('Unhandled Promise Rejection', {
          reason: event.reason
        });
      }
    };

    // Console error interception with safe logging
    const safeConsoleError = (...args: any[]) => {
      try {
        Logger.error('Console Error', { args });
      } catch {
        // Use original console.error if Logger fails
        originalConsoleError(...args);
      }
      // Always call the original console error
      originalConsoleError(...args);
    };

    // Replace console.error with safe version
    console.error = safeConsoleError;

    // Add global error listeners
    window.addEventListener('error', handleError);
    window.addEventListener('unhandledrejection', handleUnhandledRejection);

    // Log component initialization
    try {
      Logger.info('GlobalErrorHandler initialized');
    } catch {
      originalConsoleLog('GlobalErrorHandler initialized');
    }

    // Cleanup listeners on unmount
    return () => {
      window.removeEventListener('error', handleError);
      window.removeEventListener('unhandledrejection', handleUnhandledRejection);
      
      // Restore original console.error
      console.error = originalConsoleError;

      try {
        Logger.info('GlobalErrorHandler unmounted');
      } catch {
        originalConsoleLog('GlobalErrorHandler unmounted');
      }
    };
  }, []);

  // Render nothing on server, children on client
  if (!isClient) {
    return null;
  }

  return <>{children}</>;
};

export default GlobalErrorHandler;