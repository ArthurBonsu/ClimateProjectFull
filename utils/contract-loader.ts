import Web3 from 'web3';
import { Contract } from 'web3-eth-contract';
import fs from 'fs';
import path from 'path';
import contractConfig from '../config/contract_addresses.json';

// Flexible contract configuration interface
interface ContractConfig {
  address: string;
  abi?: string;
}

// Enhanced PasschainContract interface to be more compatible
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
  _address: string;
}

// Type guard to check if a contract is a PasschainContract
function isPasschainContract(contract: any): contract is PasschainContract {
  return contract && 
         typeof contract.address === 'string' && 
         typeof contract.methods === 'object' && 
         typeof contract.options === 'object';
}

// Helper function to convert Web3 Contract to PasschainContract
function convertToPasschainContract(contract: Contract<any>): PasschainContract {
  // Ensure address is always a non-empty string
  const contractAddress = contract.options.address;
  
  if (!contractAddress) {
    throw new Error('Contract address is undefined or empty');
  }

  return {
    address: contractAddress,
    methods: contract.methods,
    options: {
      address: contractAddress
    },
    _address: contractAddress
  };
}

// Helper function to find contract artifact
const findContractArtifact = (contractName: string, buildDir: string = path.join(__dirname, '..', 'build', 'contracts')): any | null => {
  const searchPaths = [
    path.join(buildDir, `${contractName}.json`),
    path.join(buildDir, 'blockchain', `${contractName}.json`),
    path.join(buildDir, 'passchain', `${contractName}.json`),
    path.join(buildDir, 'relay', `${contractName}.json`)
  ];

  for (const artifactPath of searchPaths) {
    try {
      if (fs.existsSync(artifactPath)) {
        return JSON.parse(fs.readFileSync(artifactPath, 'utf8'));
      }
    } catch (error) {
      console.warn(`Error reading artifact at ${artifactPath}:`, error);
    }
  }

  console.warn(`No artifact found for contract: ${contractName}`);
  return null;
};

export const loadContracts = (
  web3: Web3, 
  config: { [key: string]: ContractConfig } = contractConfig,
  buildDir: string = path.join(__dirname, '..', 'build', 'contracts')
): { [key: string]: PasschainContract } => {
  const contracts: { [key: string]: PasschainContract } = {};

  console.log('Starting contract loading process');
  console.log('Build Directory:', buildDir);
  console.log('Contract Configuration:', JSON.stringify(config, null, 2));

  Object.entries(config).forEach(([key, contractConfig]) => {
    try {
      // Validate contract address upfront
      if (!contractConfig.address || contractConfig.address === '0x0') {
        throw new Error(`Invalid address for contract ${key}`);
      }

      // Find the corresponding artifact
      const contractArtifact = findContractArtifact(key, buildDir);
      
      if (!contractArtifact) {
        throw new Error(`No artifact found for contract ${key}`);
      }

      // Create contract instance with validated address
      const web3Contract = new web3.eth.Contract(
        contractArtifact.abi, 
        contractConfig.address
      );

      // Validate contract instance has an address
      if (!web3Contract.options.address) {
        throw new Error(`Failed to create contract instance for ${key}`);
      }

      // Convert to PasschainContract
      const passchainContract = convertToPasschainContract(web3Contract);

      contracts[key] = passchainContract;
      
      console.log(`Successfully loaded contract ${key} at ${passchainContract.address}`);
    } catch (error) {
      console.error(`Failed to load contract ${key}:`, error instanceof Error ? error.message : error);
    }
  });

  // Throw an error if no contracts were loaded
  if (Object.keys(contracts).length === 0) {
    throw new Error('No contracts could be loaded');
  }

  return contracts;
};

export const logContractDetails = (contracts: { [key: string]: PasschainContract }) => {
  console.log('\n--- Contract Details ---');
  Object.entries(contracts).forEach(([name, contract]) => {
    console.log(`Contract: ${name}`);
    console.log(`Address: ${contract.address || contract.options.address}`);
    console.log('---');
  });
};

// Additional utility for type checking and conversion
export const ensurePasschainContract = (contract: any): PasschainContract => {
  if (isPasschainContract(contract)) {
    return contract;
  }
  
  // If it's a Web3 Contract, convert it
  if (contract && contract.options && contract.options.address) {
    return convertToPasschainContract(contract);
  }
  
  throw new Error('Unable to convert contract to PasschainContract');
};