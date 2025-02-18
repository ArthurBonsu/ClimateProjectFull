import { PasschainContract } from '../utils/contract-loader';
import Web3 from 'web3';
import { Logger } from '../utils/logger';

export class TransactionProcessor {
  private contracts: { [key: string]: PasschainContract };

  constructor(contracts: { [key: string]: PasschainContract }) {
    this.contracts = contracts;
    Logger.info('TransactionProcessor initialized', { 
      contractNames: Object.keys(contracts) 
    });
  }

  async processTransaction(transactionData: any, account: string) {
    Logger.info('Starting transaction processing', { 
      account, 
      transactionData 
    });

    try {
      // Step 1: Metadata Parsing
      const parsedMetadata = await this.contracts.MetadataParser.methods
        .parseMetadata(this.toHex(transactionData))
        .send({ from: account });
      Logger.info('Metadata parsed', { 
        transactionHash: parsedMetadata.transactionHash 
      });

      // Step 2: Speculative Transaction
      const speculativeTx = await this.contracts.PacechainChannel.methods
        .initiateSpeculativeTransaction(this.toHex(transactionData))
        .send({ from: account });
      Logger.info('Speculative transaction initiated', { 
        transactionHash: speculativeTx.transactionHash 
      });

      // Step 3: ZK Proof Generation
      const zkProof = await this.contracts.ZKPVerifierBase.methods
        .generateProof(this.toHex(transactionData), this.toHex('witness'))
        .send({ from: account });
      Logger.info('ZK Proof generated', { 
        transactionHash: zkProof.transactionHash 
      });

      // Step 4: Cluster Processing
      const clusterProcessing = await this.contracts.ClusterManager.methods
        .processTransaction(this.toHex(transactionData))
        .send({ from: account });
      Logger.info('Cluster processing completed', { 
        transactionHash: clusterProcessing.transactionHash 
      });

      // Step 5: Cross-Chain Relay
      const relayCrossChain = await this.contracts.RelayChain.methods
        .relayTransaction(this.toHex(transactionData))
        .send({ from: account });
      Logger.info('Cross-chain relay completed', { 
        transactionHash: relayCrossChain.transactionHash 
      });

      const result = {
        parsedMetadata: parsedMetadata.transactionHash,
        speculativeTx: speculativeTx.transactionHash,
        zkProof: zkProof.transactionHash,
        clusterProcessing: clusterProcessing.transactionHash,
        relayCrossChain: relayCrossChain.transactionHash
      };

      Logger.info('Transaction processing completed successfully', result);
      return result;
    } catch (error) {
      Logger.error('Transaction processing error', { 
        error, 
        transactionData 
      });
      throw error;
    }
  }

  private toHex(data: any): string {
    try {
      const hexData = Web3.utils.fromAscii(JSON.stringify(data));
      Logger.info('Data converted to hex', { 
        originalData: data, 
        hexData 
      });
      return hexData;
    } catch (error) {
      Logger.error('Error converting data to hex', { 
        error, 
        data 
      });
      throw error;
    }
  }
}