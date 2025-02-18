import React from 'react';
import dynamic from 'next/dynamic';
import Link from 'next/link';
import { ClientInit } from '@/components/ClientInit';
import TransactionForm from '@/components/TransactionForm';
import { Logger } from '@/utils/logger';

// Import BlockchainProvider with SSR disabled
const BlockchainProvider = dynamic(
  () => import('@/components/BlockchainContext').then(mod => mod.BlockchainProvider),
  { ssr: false }
);

// Create a client-side only wrapper for logging
const ClientSideLogger = ({ destination }: { destination: string }) => {
  React.useEffect(() => {
    Logger.info(`Navigating to ${destination}`);
  }, [destination]);
  
  return null;
};

// NoSSR wrapper component
const NoSSR: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [mounted, setMounted] = React.useState(false);

  React.useEffect(() => {
    setMounted(true);
  }, []);

  return mounted ? <>{children}</> : null;
};

const Home: React.FC = () => {
  const handleNavigation = (destination: string) => {
    return () => {
      Logger.info(`Navigation clicked: ${destination}`);
    };
  };

  return (
    <NoSSR>
      <BlockchainProvider>
        <ClientInit />
        <div className="container mx-auto px-4 py-8">
          <h1 className="text-4xl font-bold mb-6 text-center">
            Passchain Transaction Processor
          </h1>
          
          <div className="grid md:grid-cols-2 gap-6">
            <div className="bg-white shadow-md rounded-lg p-6">
              <h2 className="text-2xl font-semibold mb-4">Process New Transaction</h2>
              <TransactionForm />
            </div>
            
            <div className="bg-white shadow-md rounded-lg p-6">
              <h2 className="text-2xl font-semibold mb-4">Quick Actions</h2>
              <div className="space-y-4">
                <Link
                  href="/transactions"
                  className="block bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600 transition"
                  onClick={handleNavigation('Transactions History')}
                >
                  <ClientSideLogger destination="Transactions History" />
                  View Transaction History
                </Link>
                
                <Link
                  href="/performance"
                  className="block bg-green-500 text-white py-2 px-4 rounded hover:bg-green-600 transition"
                  onClick={handleNavigation('Performance Metrics')}
                >
                  <ClientSideLogger destination="Performance Metrics" />
                  Performance Metrics
                </Link>
              </div>
            </div>
          </div>
          
          <div className="mt-8 text-center">
            <p className="text-gray-600">
              Welcome to Passchain - Secure Cross-Chain Transaction Processing
            </p>
          </div>
        </div>
      </BlockchainProvider>
    </NoSSR>
  );
};

export default Home;