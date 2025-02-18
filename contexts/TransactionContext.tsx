// contexts/TransactionContext.tsx
import React, { createContext, useContext, useState, PropsWithChildren } from 'react';

export interface Transaction {
  id?: string;
  city: string;
  date: string;
  sector: string;
  ktCO2: string;
  timestamp?: number;
  blockchainResults?: {
    parsedMetadata?: string;
    speculativeTx?: string;
    zkProof?: string;
    clusterProcessing?: string;
    relayCrossChain?: string;
  };
  processingTime?: number;
}

interface TransactionContextType {
  transactions: Transaction[];
  addTransaction: (transaction: Transaction) => void;
  clearTransactions: () => void;
}

const TransactionContext = createContext<TransactionContextType>({
  transactions: [],
  addTransaction: () => {},
  clearTransactions: () => {}
});

export const TransactionProvider: React.FC<PropsWithChildren> = ({ children }) => {
  const [transactions, setTransactions] = useState<Transaction[]>([]);

  const addTransaction = (transaction: Transaction) => {
    const newTransaction = {
      ...transaction,
      id: `tx-${Date.now()}`,
      timestamp: Date.now()
    };
    setTransactions(prev => [newTransaction, ...prev]);
  };

  const clearTransactions = () => {
    setTransactions([]);
  };

  return (
    <TransactionContext.Provider value={{ 
      transactions, 
      addTransaction, 
      clearTransactions 
    }}>
      {children}
    </TransactionContext.Provider>
  );
};

export const useTransactions = () => useContext(TransactionContext);