export const trackPerformance = (func: Function) => {
    return async (...args: any[]) => {
      const start = performance.now();
      try {
        const result = await func(...args);
        const end = performance.now();
        
        console.log(`Performance Metrics:
          - Execution Time: ${end - start}ms
          - Memory Used: ${process.memoryUsage().heapUsed / 1024 / 1024} MB
        `);
        
        return result;
      } catch (error) {
        console.error('Performance tracking error', error);
        throw error;
      }
    };
  };
  
  export class PerformanceTracker {
    private startTime: number;
    private steps: { name: string, timestamp: number }[] = [];
    private memorySnapshots: { name: string, memory: number }[] = [];
  
    constructor() {
      this.startTime = performance.now();
    }
  
    addStep(name: string) {
      this.steps.push({
        name,
        timestamp: performance.now() - this.startTime
      });
      this.takeMemorySnapshot(name);
    }
  
    takeMemorySnapshot(name: string) {
      this.memorySnapshots.push({
        name,
        memory: process.memoryUsage().heapUsed / 1024 / 1024
      });
    }
  
    getPerformanceMetrics() {
      const totalTime = performance.now() - this.startTime;
      return {
        totalExecutionTime: totalTime,
        steps: this.steps,
        memorySnapshots: this.memorySnapshots,
        finalMemoryUsed: process.memoryUsage().heapUsed / 1024 / 1024
      };
    }
  
    // Decorator method for tracking performance of async functions
    static track<T>(fn: (...args: any[]) => Promise<T>): (...args: any[]) => Promise<T> {
      return async (...args: any[]) => {
        const tracker = new PerformanceTracker();
        try {
          tracker.addStep('Function started');
          const result = await fn(...args);
          tracker.addStep('Function completed');
          
          const metrics = tracker.getPerformanceMetrics();
          console.log('Performance Metrics:', metrics);
          
          return result;
        } catch (error) {
          tracker.addStep('Function failed');
          console.error('Performance tracking error', error);
          throw error;
        }
      };
    }
  
    // Method to compare performance between two function executions
    static async comparePerformance<T>(
      func1: (...args: any[]) => Promise<T>,
      func2: (...args: any[]) => Promise<T>,
      ...args: any[]
    ): Promise<{
      func1Metrics: ReturnType<PerformanceTracker['getPerformanceMetrics']>,
      func2Metrics: ReturnType<PerformanceTracker['getPerformanceMetrics']>
    }> {
      const tracker1 = new PerformanceTracker();
      const tracker2 = new PerformanceTracker();
  
      try {
        tracker1.addStep('Func1 started');
        const result1 = await func1(...args);
        tracker1.addStep('Func1 completed');
        const func1Metrics = tracker1.getPerformanceMetrics();
  
        tracker2.addStep('Func2 started');
        const result2 = await func2(...args);
        tracker2.addStep('Func2 completed');
        const func2Metrics = tracker2.getPerformanceMetrics();
  
        return {
          func1Metrics,
          func2Metrics
        };
      } catch (error) {
        console.error('Performance comparison error', error);
        throw error;
      }
    }
  }
  
  // Example usage
  async function exampleFunction() {
    // Some async operation
    await new Promise(resolve => setTimeout(resolve, 100));
    return 'Function completed';
  }
  
  // Using the decorator
  const trackedFunction = PerformanceTracker.track(exampleFunction);
  
  // Using the functional approach
  const performanceTrackedFunction = trackPerformance(exampleFunction);