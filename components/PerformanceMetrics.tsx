import React from 'react';
import { PerformanceTracker } from '../utils/performance-tracker';
import { Logger } from '../utils/logger';

interface PerformanceMetricsProps {
  performanceData: ReturnType<PerformanceTracker['getPerformanceMetrics']>;
}

const PerformanceMetrics: React.FC<PerformanceMetricsProps> = ({ performanceData }) => {
  // Log component initialization
  React.useEffect(() => {
    Logger.info('PerformanceMetrics component initialized', {
      totalExecutionTime: performanceData.totalExecutionTime,
      memoryUsed: performanceData.finalMemoryUsed
    });
  }, [performanceData]);

  return (
    <div className="performance-metrics">
      <h2>Performance Metrics</h2>
      <p>Total Execution Time: {performanceData.totalExecutionTime.toFixed(2)} ms</p>
      <p>Memory Used: {performanceData.finalMemoryUsed.toFixed(2)} MB</p>
      
      <h3>Execution Steps</h3>
      <ul>
        {performanceData.steps.map((step, index) => (
          <li key={index}>
            {step.name} - {step.timestamp.toFixed(2)} ms
          </li>
        ))}
      </ul>

      <h3>Memory Snapshots</h3>
      <ul>
        {performanceData.memorySnapshots.map((snapshot, index) => (
          <li key={index}>
            {snapshot.name}: {snapshot.memory.toFixed(2)} MB
          </li>
        ))}
      </ul>
    </div>
  );
};

export default PerformanceMetrics;