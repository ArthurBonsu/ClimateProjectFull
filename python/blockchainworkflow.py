# blockchain_workflow.py

import pandas as pd
from web3 import Web3
import json
import logging
from datetime import datetime
import numpy as np
from pathlib import Path
import os

class BlockchainWorkflow:
    def __init__(self, network="development"):
        """
        Initialize BlockchainWorkflow with network selection
        network options: "development", "ethereum_testnet", "tatum_testnet"
        """
        self.setup_logging()
        self.network = network
        self.w3 = self.setup_web3()
        self.contracts = {}
        self.load_contract_addresses()
        
    def setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def setup_web3(self):
        """Setup Web3 connection based on network"""
        if self.network == "development":
            return Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
        elif self.network == "ethereum_testnet":
            infura_url = f"https://sepolia.infura.io/v3/{os.getenv('INFURA_PROJECT_ID')}"
            return Web3(Web3.HTTPProvider(infura_url))
        else:
            tatum_url = "https://ethereum-sepolia.gateway.tatum.io/"
            return Web3(Web3.HTTPProvider(tatum_url))

    def load_contract_addresses(self):
        """Load deployed contract addresses from Truffle artifacts"""
        try:
            network_id = self.w3.net.version
            contracts_dir = Path('./build/contracts')
            
            # Map contract names to their files
            contract_files = {
                'CityRegister': 'CityRegister.json',
                'CompanyRegister': 'CompanyRegister.json',
                'CityEmissionsContract': 'CityEmissionsContract.json',
                'RenewalTheoryContract': 'RenewalTheoryContract.json',
                'CityHealthCalculator': 'CityHealthCalculator.json',
                'TemperatureRenewalContract': 'TemperatureRenewalContract.json',
                'MitigationContract': 'MitigationContract.json'
            }

            self.contract_addresses = {}
            for name, filename in contract_files.items():
                filepath = contracts_dir / filename
                if filepath.exists():
                    with open(filepath) as f:
                        contract_data = json.load(f)
                        if network_id in contract_data['networks']:
                            self.contract_addresses[name] = contract_data['networks'][network_id]['address']
                            
            logging.info(f"Loaded contract addresses for network {network_id}")
        except Exception as e:
            logging.error(f"Error loading contract addresses: {str(e)}")
            raise

    async def load_contract(self, contract_name, abi_path):
        """Load contract instance using ABI"""
        try:
            with open(abi_path) as f:
                contract_json = json.load(f)
            
            contract_address = self.contract_addresses[contract_name]
            contract = self.w3.eth.contract(
                address=contract_address,
                abi=contract_json['abi']
            )
            self.contracts[contract_name] = contract
            logging.info(f"Loaded contract {contract_name}")
            return contract
        except Exception as e:
            logging.error(f"Error loading contract {contract_name}: {str(e)}")
            raise

    async def process_melbourne_data(self, csv_path):
        """Process Melbourne aviation data"""
        try:
            df = pd.read_csv(csv_path)
            
            # Convert date strings to timestamps
            df['timestamp'] = pd.to_datetime(df['date']).astype(np.int64) // 10**9
            
            # Group by date and calculate daily totals
            daily_emissions = df.groupby(['city', 'date'])['value'].sum().reset_index()
            
            contract = self.contracts['CityEmissionsContract']
            
            for _, row in daily_emissions.iterrows():
                # Convert emission value to Wei
                emission_value = self.w3.to_wei(float(row['value']), 'ether')
                
                tx_hash = await contract.functions.addEmissionData(
                    row['city'],
                    int(row['timestamp']),
                    'Aviation',
                    emission_value,
                    80  # Default AQI
                ).transact({
                    'from': self.w3.eth.accounts[0],
                    'gas': 2000000
                })
                
                receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
                logging.info(f"Processed emissions for {row['city']} on {row['date']}")
                self.log_to_file('emissions_logs.json', row.to_dict(), receipt)
                
            await self.calculate_renewal_metrics()
            
        except Exception as e:
            logging.error(f"Error processing Melbourne data: {str(e)}")
            raise

    async def calculate_renewal_metrics(self):
        """Calculate renewal theory metrics"""
        try:
            contract = self.contracts['RenewalTheoryContract']
            
            tx_hash = await contract.functions.processRenewal(
                "Melbourne",
                "Aviation"
            ).transact({
                'from': self.w3.eth.accounts[0],
                'gas': 2000000
            })
            
            receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
            logging.info("Calculated renewal metrics for Melbourne Aviation sector")
            
            # Get the renewal metrics
            metrics = await contract.functions.getSectorStats(
                "Melbourne",
                "Aviation"
            ).call()
            
            return metrics
            
        except Exception as e:
            logging.error(f"Error calculating renewal metrics: {str(e)}")
            raise

    def log_to_file(self, filename, data, receipt):
        """Log transaction data to file"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'data': data,
            'gas_used': receipt['gasUsed'],
            'transaction_hash': receipt['transactionHash'].hex()
        }
        
        with open(filename, 'a') as f:
            json.dump(log_entry, f)
            f.write('\n')

    async def run_complete_workflow(self, melbourne_data_path):
        """Run the complete workflow"""
        try:
            # 1. Load all contracts
            await self.load_all_contracts()
            
            # 2. Process Melbourne data
            await self.process_melbourne_data(melbourne_data_path)
            
            # 3. Generate summary report
            report = self.generate_summary_report()
            
            logging.info("Complete workflow executed successfully")
            return report
            
        except Exception as e:
            logging.error(f"Error in workflow execution: {str(e)}")
            raise