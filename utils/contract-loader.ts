import Web3 from 'web3';
import { Contract as Web3Contract } from 'web3-eth-contract';
import contractConfig from '../config/contract_addresses.json';

export interface PasschainContract {
  address: string;
  methods: {
    [key: string]: (...args: any[]) => {
      send: (options?: any) => Promise<any>;
      call: (options?: any) => Promise<any>;
    };
  };
  options: {
    address: string;
  };
  _address?: string;
}

export const loadContracts = (web3: Web3): { [key: string]: PasschainContract } => {
  const contracts: { [key: string]: PasschainContract } = {};

  Object.entries(contractConfig).forEach(([name, config]) => {
    try {
      // Load contract using address from contract_addresses.json
      const contractArtifact = require(`../build/contracts/${config.abi}`);
      
      // Create contract instance
      const contract = new web3.eth.Contract(
        contractArtifact.abi, 
        config.address
      ) as unknown as PasschainContract;

      // Ensure address is set
      contract.address = config.address;
      contract.options = { address: config.address };

      contracts[name] = contract;
    } catch (error) {
      console.warn(`Failed to load contract ${name}:`, error);
    }
  });

  return contracts;
};

export const logContractDetails = (contracts: { [key: string]: PasschainContract }) => {
  Object.entries(contracts).forEach(([name, contract]) => {
    console.log(`Contract: ${name}`);
    console.log(`Address: ${contract.address || contract.options.address}`);
  });
};