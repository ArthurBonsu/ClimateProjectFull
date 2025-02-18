import Web3 from 'web3';
import { PasschainContract } from '../utils/contract-loader';
import { Logger } from '../utils/logger';
import networkConfig, { NetworkConfig } from '../config/network_config';

// Define a type for valid network names
type NetworkName = keyof typeof networkConfig.networks;

interface NetworkSwitchOptions {
  chainId?: number;
  networkName?: NetworkName;
}

interface TransactionPerformanceMetrics {
  startTime: number;
  endTime: number;
  totalDuration: number;
  stages: {
    name: string;
    startTime: number;
    endTime: number;
    duration: number;
  }[];
}

export class BlockchainService {
  private web3: Web3;
  private contracts: { [key: string]: PasschainContract };
  private currentNetwork: NetworkName;

  constructor(web3: Web3, contracts: { [key: string]: PasschainContract }) {
    this.web3 = web3;
    this.contracts = contracts;
    this.currentNetwork = networkConfig.default as NetworkName;
    Logger.info('BlockchainService initialized', { 
      contractCount: Object.keys(contracts).length,
      initialNetwork: this.currentNetwork
    });
  }

  // Enhanced Network Switching with Performance Tracking
  async switchNetwork(options: NetworkSwitchOptions): Promise<TransactionPerformanceMetrics> {
    const performanceMetrics: TransactionPerformanceMetrics = {
      startTime: performance.now(),
      endTime: 0,
      totalDuration: 0,
      stages: []
    };

    const stageStart = (name: string) => {
      const stageStartTime = performance.now();
      return {
        name,
        startTime: stageStartTime,
        endTime: 0,
        duration: 0
      };
    };

    const stageEnd = (stage: any) => {
      stage.endTime = performance.now();
      stage.duration = stage.endTime - stage.startTime;
      performanceMetrics.stages.push(stage);
      return stage;
    };

    try {
      // Determine target network
      let targetNetwork: NetworkName | undefined;
      const networkStage = stageStart('Network Determination');
      
      if (options.networkName && options.networkName in networkConfig.networks) {
        targetNetwork = options.networkName;
      } else if (options.chainId) {
        targetNetwork = this.getNetworkNameByChainId(options.chainId);
      }

      if (!targetNetwork) {
        throw new Error('Invalid network specification');
      }
      stageEnd(networkStage);

      // Validate Network Configuration
      const networkValidationStage = stageStart('Network Validation');
      const networkDetails = networkConfig.networks[targetNetwork];
      if (!networkDetails) {
        throw new Error(`Network ${targetNetwork} not configured`);
      }
      stageEnd(networkValidationStage);

      // Network Switching Stage
      const switchStage = stageStart('Network Switch');
      if ((window as any).ethereum) {
        await (window as any).ethereum.request({
          method: 'wallet_switchEthereumChain',
          params: [{ chainId: `0x${networkDetails.chainId.toString(16)}` }]
        });
      }

      // Update Web3 provider
      this.web3.setProvider(networkDetails.provider);
      this.currentNetwork = targetNetwork;
      stageEnd(switchStage);

      // Final Performance Calculation
      performanceMetrics.endTime = performance.now();
      performanceMetrics.totalDuration = performanceMetrics.endTime - performanceMetrics.startTime;

      Logger.info('Network switched with detailed performance', { 
        newNetwork: targetNetwork, 
        chainId: networkDetails.chainId,
        performanceMetrics 
      });

      return performanceMetrics;
    } catch (error) {
      performanceMetrics.endTime = performance.now();
      performanceMetrics.totalDuration = performanceMetrics.endTime - performanceMetrics.startTime;

      Logger.error('Network switch failed', { 
        error, 
        performanceMetrics 
      });
      throw error;
    }
  }

  // Enhanced Cross-Chain Transaction Routing with Detailed Performance Tracking
  async routeCrossChainTransaction(
    sourceNetwork: NetworkName, 
    targetNetwork: NetworkName, 
    transactionData: any
  ): Promise<{
    result: any,
    performanceMetrics: TransactionPerformanceMetrics
  }> {
    const performanceMetrics: TransactionPerformanceMetrics = {
      startTime: performance.now(),
      endTime: 0,
      totalDuration: 0,
      stages: []
    };

    const stageStart = (name: string) => {
      const stageStartTime = performance.now();
      return {
        name,
        startTime: stageStartTime,
        endTime: 0,
        duration: 0
      };
    };

    const stageEnd = (stage: any) => {
      stage.endTime = performance.now();
      stage.duration = stage.endTime - stage.startTime;
      performanceMetrics.stages.push(stage);
      return stage;
    };

    try {
      // Network Validation Stage
      const networkValidationStage = stageStart('Network Validation');
      if (!networkConfig.networks[sourceNetwork] || 
          !networkConfig.networks[targetNetwork]) {
        throw new Error('Invalid source or target network');
      }
      stageEnd(networkValidationStage);

      // Transaction Validation Stage
      const validationStage = stageStart('Transaction Validation');
      const sourceValidation = await this.validateTransaction(
        sourceNetwork, 
        transactionData
      );
      stageEnd(validationStage);

      // Transaction Preparation Stage
      const preparationStage = stageStart('Transaction Preparation');
      const preparedTransaction = await this.prepareForTargetNetwork(
        sourceNetwork, 
        targetNetwork, 
        transactionData
      );
      stageEnd(preparationStage);

      // Transaction Execution Stage
      const executionStage = stageStart('Transaction Execution');
      const result = await this.executeOnTargetNetwork(
        targetNetwork, 
        preparedTransaction
      );
      stageEnd(executionStage);

      // Final Performance Calculation
      performanceMetrics.endTime = performance.now();
      performanceMetrics.totalDuration = performanceMetrics.endTime - performanceMetrics.startTime;

      Logger.info('Cross-chain transaction completed', { 
        source: sourceNetwork, 
        target: targetNetwork,
        performanceMetrics 
      });

      return { result, performanceMetrics };
    } catch (error) {
      performanceMetrics.endTime = performance.now();
      performanceMetrics.totalDuration = performanceMetrics.endTime - performanceMetrics.startTime;

      Logger.error('Cross-chain transaction failed', { 
        error, 
        performanceMetrics 
      });
      throw error;
    }
  }

  // Helper method to get network name by chain ID
  private getNetworkNameByChainId(chainId: number): NetworkName | undefined {
    const entry = Object.entries(networkConfig.networks).find(
      ([, config]) => config.chainId === chainId
    );
    return entry ? entry[0] as NetworkName : undefined;
  }

  // Placeholder methods for cross-chain transaction routing
  private async validateTransaction(network: NetworkName, data: any): Promise<boolean> {
    // Implement network-specific transaction validation
    return true;
  }

  private async prepareForTargetNetwork(
    sourceNetwork: NetworkName, 
    targetNetwork: NetworkName, 
    data: any
  ): Promise<any> {
    // Implement transaction preparation logic
    Logger.info('Preparing transaction for target network', {
      sourceNetwork,
      targetNetwork,
      data
    });

    // Example: Add network-specific metadata
    const preparedData = {
      ...data,
      sourceNetwork,
      targetNetwork,
      timestamp: Date.now(),
      // You might want to add more network-specific details here
    };

    // Example: Encode the data for cross-chain transfer
    const encodedData = this.web3.eth.abi.encodeParameters(
      ['string', 'string', 'uint256', 'bytes'],
      [preparedData.sourceNetwork, preparedData.targetNetwork, preparedData.timestamp, JSON.stringify(data)]
    );

    return encodedData;
  }

  private async executeOnTargetNetwork(
    network: NetworkName, 
    preparedTransaction: any
  ): Promise<any> {
    Logger.info('Executing transaction on target network', {
      network,
      preparedTransaction
    });
  
    try {
      // Decode the prepared transaction data
      const decodedParams = this.web3.eth.abi.decodeParameters(
        ['string', 'string', 'uint256', 'bytes'],
        preparedTransaction
      );
  
      const sourceNetwork = decodedParams['0'];
      const targetNetwork = decodedParams['1'];
      const timestamp = decodedParams['2'];
      const originalData = decodedParams['3'];
  
      // Get the appropriate contract for the target network
      const targetContract = this.contracts['TransactionRelay'];
  
      if (!targetContract) {
        throw new Error('TransactionRelay contract not found');
      }
  
      // Call a method on the target contract to execute the transaction
      const result = await targetContract.methods.executeRemoteTransaction(
        sourceNetwork,
        targetNetwork,
        timestamp,
        originalData
      ).send({ from: await this.web3.eth.getAccounts().then(accounts => accounts[0]) });
  
      Logger.info('Transaction executed successfully', { result });
  
      return result;
    } catch (error) {
      Logger.error('Failed to execute transaction on target network', { error });
      throw error;
    }
  }}