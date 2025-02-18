import axios from 'axios';
import Web3 from 'web3';
import { getTatumNetworkConfig } from '../config/tatum-config';

export interface TatumConnectionOptions {
  apiKey?: string;
  network?: string;
}

export class TatumBlockchainService {
  private apiKey: string;
  private web3: Web3;
  private networkConfig;

  constructor(options: TatumConnectionOptions = {}) {
    const apiKey = options.apiKey || process.env.NEXT_PUBLIC_TATUM_API_KEY;
    const network = options.network || 'sepolia';

    if (!apiKey) {
      throw new Error('Tatum API Key is required');
    }

    this.apiKey = apiKey;
    this.networkConfig = getTatumNetworkConfig(network);
    
    // Prefer Tatum RPC URL, fallback to provider URL
    const providerUrl = this.networkConfig.rpcUrl || this.networkConfig.providerUrl;
    
    this.web3 = new Web3(
      new Web3.providers.HttpProvider(providerUrl!, {
        headers: [{ name: 'x-api-key', value: this.apiKey }]
      })
    );
  }

  // Comprehensive connection test
  async testConnection(): Promise<{
    connected: boolean;
    blockNumber?: number;
    networkInfo?: any;
    accounts?: string[];
  }> {
    try {
      // Test block number retrieval
      const blockNumber = await this.getCurrentBlockNumber();
      
      // Attempt to retrieve accounts
      let accounts: string[] = [];
      try {
        accounts = await this.getAccounts();
      } catch (accountError) {
        console.warn('Could not retrieve accounts:', accountError);
      }

      return {
        connected: true,
        blockNumber,
        networkInfo: {
          name: this.networkConfig.name,
          chainId: this.networkConfig.chainId,
          type: this.networkConfig.type
        },
        accounts
      };
    } catch (error) {
      console.error('Tatum connection test failed:', error);
      return { 
        connected: false,
        accounts: []
      };
    }
  }

  // Get accounts with error handling
  async getAccounts(): Promise<string[]> {
    try {
      return await this.web3.eth.getAccounts();
    } catch (error) {
      console.error('Failed to retrieve accounts:', error);
      return [];
    }
  }

  // Get current block number
  async getCurrentBlockNumber(): Promise<number> {
    try {
      // Ensure rpcUrl exists
      const rpcUrl = this.networkConfig.rpcUrl || this.networkConfig.providerUrl;
      
      if (!rpcUrl) {
        throw new Error('No RPC URL available for block number retrieval');
      }
  
      const response = await axios.post(
        rpcUrl,
        {
          jsonrpc: '2.0',
          method: 'eth_blockNumber',
          params: [],
          id: 1
        },
        {
          headers: {
            'x-api-key': this.apiKey,
            'Content-Type': 'application/json'
          }
        }
      );
  
      // Ensure response and result exist
      if (!response.data || !response.data.result) {
        throw new Error('Invalid response from RPC');
      }
  
      return parseInt(response.data.result, 16);
    } catch (error) {
      console.error('Failed to get block number:', error);
      
      // Fallback to Web3 method if axios fails
      try {
        return await this.web3.eth.getBlockNumber();
      } catch (web3Error) {
        console.error('Web3 block number retrieval failed:', web3Error);
        throw error;
      }
    }
  }

  // Get balance of an address
  async getBalance(address: string): Promise<string> {
    try {
      const balanceWei = await this.web3.eth.getBalance(address);
      return this.web3.utils.fromWei(balanceWei, 'ether');
    } catch (error) {
      console.error(`Failed to get balance for ${address}:`, error);
      throw error;
    }
  }

  // Deploy a contract with comprehensive error handling
  async deployContract(
    abi: any[], 
    bytecode: string, 
    from: string, 
    privateKey: string
  ): Promise<{ 
    contractAddress: string, 
    transactionHash: string,
    blockNumber: number 
  }> {
    try {
      // Validate inputs
      if (!abi || !bytecode || !from || !privateKey) {
        throw new Error('Missing required deployment parameters');
      }
  
      // Create contract instance
      const contract = new this.web3.eth.Contract(abi);
  
      // Estimate gas
      const gasEstimate = await contract
        .deploy({ data: bytecode })
        .estimateGas({ from });
  
      // Get current gas price
      const gasPrice = await this.web3.eth.getGasPrice();
  
      // Sign the transaction
      const signedTx = await this.web3.eth.accounts.signTransaction(
        {
          from,
          data: bytecode,
          gas: Math.floor(gasEstimate * 1.2), // Add 20% buffer
          gasPrice
        },
        privateKey
      );
  
      // Ensure rawTransaction exists
      if (!signedTx.rawTransaction) {
        throw new Error('Transaction signing failed: No raw transaction');
      }
  
      // Send the transaction
      const receipt = await this.web3.eth.sendSignedTransaction(
        signedTx.rawTransaction
      );
  
      // Validate receipt
      if (!receipt.contractAddress) {
        throw new Error('Contract deployment failed: No contract address in receipt');
      }
  
      return {
        contractAddress: receipt.contractAddress,
        transactionHash: receipt.transactionHash,
        blockNumber: receipt.blockNumber || 0
      };
    } catch (error) {
      console.error('Contract deployment failed:', error);
      
      // Enhanced error logging
      if (error instanceof Error) {
        console.error('Error details:', {
          message: error.message,
          stack: error.stack
        });
      }
      
      throw error;
    }
  }

  // Advanced transaction sending with detailed logging
  async sendTransaction(
    from: string, 
    to: string, 
    amount: string, 
    privateKey: string,
    options?: {
      gasPrice?: string;
      gasLimit?: number;
      nonce?: number;
    }
  ): Promise<{
    transactionHash: string;
    blockNumber?: number;
    gasUsed?: number;
  }> {
    try {
      // Validate inputs
      if (!from || !to || !amount || !privateKey) {
        throw new Error('Missing required transaction parameters');
      }
  
      // Get nonce and gas price
      const nonce = options?.nonce || await this.web3.eth.getTransactionCount(from);
      const gasPrice = options?.gasPrice || await this.web3.eth.getGasPrice();
  
      // Prepare transaction
      const tx = {
        from,
        to,
        value: this.web3.utils.toWei(amount, 'ether'),
        gas: options?.gasLimit || 21000, // Standard gas limit for ETH transfer
        gasPrice,
        nonce
      };
  
      // Sign transaction
      const signedTx = await this.web3.eth.accounts.signTransaction(tx, privateKey);
  
      // Ensure rawTransaction exists
      if (!signedTx.rawTransaction) {
        throw new Error('Transaction signing failed: No raw transaction');
      }
  
      // Send signed transaction
      const receipt = await this.web3.eth.sendSignedTransaction(
        signedTx.rawTransaction
      );
  
      // Validate receipt
      if (!receipt.transactionHash) {
        throw new Error('Transaction submission failed: No transaction hash');
      }
  
      return {
        transactionHash: receipt.transactionHash,
        blockNumber: receipt.blockNumber || undefined,
        gasUsed: receipt.gasUsed || undefined
      };
    } catch (error) {
      // Enhanced error logging
      console.error('Transaction failed:', error);
      
      if (error instanceof Error) {
        console.error('Error details:', {
          message: error.message,
          stack: error.stack
        });
      }
  
      // Rethrow or handle specific error types
      throw error;
    }
  }

  // Get detailed transaction details
  async getTransactionDetails(txHash: string) {
    try {
      const transaction = await this.web3.eth.getTransaction(txHash);
      const receipt = await this.web3.eth.getTransactionReceipt(txHash);

      return {
        ...transaction,
        receipt,
        explorerUrl: `${this.networkConfig.explorerUrl}/tx/${txHash}`
      };
    } catch (error) {
      console.error(`Failed to get transaction details for ${txHash}:`, error);
      throw error;
    }
  }

  // Utility method to interact with specific contracts
  getContractInstance(address: string, abi: any[]) {
    return new this.web3.eth.Contract(abi, address);
  }
}

// Factory function with improved type safety and configuration
export function createTatumBlockchainService(
  options: TatumConnectionOptions = {}
): TatumBlockchainService {
  return new TatumBlockchainService(options);
}

// Export a pre-configured service for easy import
export const tatumService = createTatumBlockchainService();