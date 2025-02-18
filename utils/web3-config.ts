import Web3 from 'web3';
import { loadContracts } from './contract-loader';

export class Web3Config {
  private static instance: Web3Config;
  public web3: Web3;
  public contracts: ReturnType<typeof loadContracts>;

  private constructor() {
    // Use Metamask or fallback to local provider
    const provider = 
      typeof window !== 'undefined' && (window as any).ethereum 
        ? new Web3.providers.HttpProvider((window as any).ethereum)
        : new Web3.providers.HttpProvider('http://localhost:8545');

    this.web3 = new Web3(provider);
    this.contracts = loadContracts(this.web3);
  }

  public static getInstance(): Web3Config {
    if (!Web3Config.instance) {
      Web3Config.instance = new Web3Config();
    }
    return Web3Config.instance;
  }

  async connectWallet(): Promise<string[]> {
    if (typeof window !== 'undefined' && (window as any).ethereum) {
      try {
        // Request account access
        await (window as any).ethereum.request({ method: 'eth_requestAccounts' });
        return await this.web3.eth.getAccounts();
      } catch (error) {
        console.error('Failed to connect wallet', error);
        throw error;
      }
    } else {
      throw new Error('Ethereum wallet not found');
    }
  }
}