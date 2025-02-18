import React, { useState, useEffect } from 'react';
import { useBlockchain } from '../components/BlockchainContext';
import { useTransactions } from '@/contexts/TransactionContext';
import { Logger } from '../utils/logger';

const TransactionForm: React.FC = () => {
  const { web3, accounts, connect, processTransaction } = useBlockchain();
  const { addTransaction } = useTransactions();
  const [isClient, setIsClient] = useState(false);
  const [isConnecting, setIsConnecting] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);
  const [formData, setFormData] = useState({
    city: '',
    date: '',
    sector: '',
    ktCO2: ''
  });
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setIsClient(true);
  }, []);

  useEffect(() => {
    // Log connection status changes
    Logger.info('Connection status', {
      isClient,
      web3Exists: !!web3,
      accountsCount: accounts?.length || 0,
      contractsInitialized: !!processTransaction
    });
  }, [web3, accounts, isClient]);

  if (!isClient) {
    return null;
  }

  const handleConnect = async () => {
    setIsConnecting(true);
    setError(null);
    
    try {
      Logger.info('Attempting wallet connection');
      await connect();
      Logger.info('Wallet connected successfully');
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to connect wallet';
      Logger.error('Wallet connection error', { error: errorMessage, details: err });
      setError(errorMessage);
    } finally {
      setIsConnecting(false);
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!web3 || accounts.length === 0) {
      await handleConnect();
      if (!web3 || accounts.length === 0) {
        return; // Exit if connection failed
      }
    }

    setIsProcessing(true);
    setError(null);
    const startTime = performance.now();
    
    Logger.info('Transaction form submitted', { 
      formData,
      connectionStatus: {
        web3Exists: !!web3,
        accountsCount: accounts?.length || 0
      }
    });

    try {
      const transactionResult = await processTransaction(formData);
      const endTime = performance.now();
      const processingTime = endTime - startTime;

      const transactionData = {
        ...formData,
        blockchainResults: transactionResult,
        processingTime,
        timestamp: Date.now()
      };

      addTransaction(transactionData);
      setResult(transactionResult);
      setFormData({ city: '', date: '', sector: '', ktCO2: '' }); // Clear form

      Logger.info('Transaction processed successfully', { 
        transactionResult,
        processingTime,
        transactionData
      });
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'An unknown error occurred';
      Logger.error('Transaction processing error', { 
        error: errorMessage, 
        details: err,
        formData 
      });
      setError(errorMessage);
    } finally {
      setIsProcessing(false);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const isWalletConnected = web3 && accounts.length > 0;

  return (
    <div className="transaction-form">
      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <input 
            type="text" 
            name="city" 
            placeholder="City" 
            value={formData.city}
            onChange={handleChange}
            className="w-full p-2 border rounded"
            required 
            disabled={isProcessing}
          />
          <input 
            type="date" 
            name="date" 
            value={formData.date}
            onChange={handleChange}
            className="w-full p-2 border rounded"
            required 
            disabled={isProcessing}
          />
        </div>
        <div className="grid grid-cols-2 gap-4">
          <input 
            type="text" 
            name="sector" 
            placeholder="Sector" 
            value={formData.sector}
            onChange={handleChange}
            className="w-full p-2 border rounded"
            required 
            disabled={isProcessing}
          />
          <input 
            type="number" 
            name="ktCO2" 
            placeholder="ktCO2" 
            value={formData.ktCO2}
            onChange={handleChange}
            step="0.00000001"
            className="w-full p-2 border rounded"
            required 
            disabled={isProcessing}
          />
        </div>
        <button 
          type="submit"
          className={`w-full p-2 rounded transition ${
            isProcessing || isConnecting
              ? 'bg-gray-400 cursor-not-allowed'
              : isWalletConnected
              ? 'bg-blue-500 hover:bg-blue-600 text-white'
              : 'bg-green-500 hover:bg-green-600 text-white'
          }`}
          disabled={isProcessing || isConnecting}
        >
          {isProcessing ? 'Processing...' : 
           isConnecting ? 'Connecting...' :
           isWalletConnected ? 'Process Transaction' : 'Connect Wallet'}
        </button>
      </form>

      {error && (
        <div className="mt-4 p-2 bg-red-100 text-red-700 rounded">
          <p className="font-medium">Error:</p>
          <p>{error}</p>
        </div>
      )}
      
      {result && (
        <div className="mt-4 p-4 bg-green-50 rounded">
          <h2 className="text-xl font-semibold mb-2">Transaction Results</h2>
          <pre className="overflow-x-auto bg-gray-100 p-2 rounded">
            {JSON.stringify(result, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
};

export default TransactionForm;