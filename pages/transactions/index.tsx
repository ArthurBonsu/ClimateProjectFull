// pages/transactions/index.tsx
import React, { useState, useEffect } from 'react';
import { useBlockchain } from '../../components/BlockchainContext';
import { useTransactions } from '@/contexts/TransactionContext';

const TransactionList: React.FC = () => {
  const { transactions } = useTransactions();
  const { contracts, web3 } = useBlockchain();
  const [blockchainTransactions, setBlockchainTransactions] = useState<any[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchBlockchainTransactions = async () => {
      if (!contracts.RelayChain || !web3) {
        setLoading(false);
        return;
      }

      try {
        const txCount = await contracts.RelayChain.methods.getTransactionCount().call();
        const txList = await Promise.all(
          Array.from({length: Number(txCount)}, (_, i) => 
            contracts.RelayChain.methods.getTransaction(i).call()
          )
        );
        setBlockchainTransactions(txList);
      } catch (error) {
        console.error('Failed to fetch blockchain transactions', error);
        setError('Failed to load blockchain transactions');
      } finally {
        setLoading(false);
      }
    };

    fetchBlockchainTransactions();
  }, [contracts, web3]);

  if (loading) {
    return (
      <div className="container mx-auto px-4 py-8 text-center">
        <p className="text-xl">Loading transactions...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mx-auto px-4 py-8 text-center text-red-500">
        <p className="text-xl">Error: {error}</p>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <h1 className="text-3xl font-bold mb-6">Transaction History</h1>
      
      <div className="mb-6">
        <h2 className="text-2xl font-semibold mb-4">Local Transactions</h2>
        {transactions.length === 0 ? (
          <p className="text-gray-600">No local transactions found</p>
        ) : (
          <div className="grid gap-4">
            {transactions.map((tx, index) => (
              <div 
                key={tx.id || index} 
                className="bg-white shadow-md rounded-lg p-6"
              >
                <h3 className="text-xl font-semibold mb-4">
                  Local Transaction {index + 1}
                </h3>
                
                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <p><strong>City:</strong> {tx.city}</p>
                    <p><strong>Date:</strong> {tx.date}</p>
                    <p><strong>Sector:</strong> {tx.sector}</p>
                    <p><strong>ktCO2:</strong> {tx.ktCO2}</p>
                  </div>
                  
                  <div>
                    <p><strong>Timestamp:</strong> {new Date(tx.timestamp || 0).toLocaleString()}</p>
                    <p><strong>Processing Time:</strong> {tx.processingTime?.toFixed(2)} ms</p>
                  </div>
                </div>

                {tx.blockchainResults && (
                  <details className="mt-4">
                    <summary>Blockchain Transaction Details</summary>
                    <pre className="bg-gray-100 p-4 rounded-md overflow-x-auto">
                      {JSON.stringify(tx.blockchainResults, null, 2)}
                    </pre>
                  </details>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      <div>
        <h2 className="text-2xl font-semibold mb-4">Blockchain Transactions</h2>
        {blockchainTransactions.length === 0 ? (
          <p className="text-gray-600">No blockchain transactions found</p>
        ) : (
          <div className="grid gap-4">
            {blockchainTransactions.map((tx, index) => (
              <div 
                key={tx.id || index} 
                className="bg-white shadow-md rounded-lg p-6"
              >
                <h3 className="text-xl font-semibold mb-4">
                  Blockchain Transaction {index + 1}
                </h3>
                
                <details>
                  <summary>Transaction Details</summary>
                  <pre className="bg-gray-100 p-4 rounded-md overflow-x-auto">
                    {JSON.stringify(tx, null, 2)}
                  </pre>
                </details>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default TransactionList;