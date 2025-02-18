import os
import logging
import asyncio
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from web3 import Web3

# Import custom modules
from city_module import CityModule
from company_module import CompanyModule
from emissions_module import EmissionsModule
from health_module import HealthModule
from renewal_module import RenewalModule

class BlockchainWorkflow:
    def __init__(self, network="development"):
        """
        Initialize blockchain workflow with network configuration
        
        :param network: Blockchain network to connect to (development, sepolia, etc.)
        """
        # Setup logging
        self.setup_logging()
        
        # Network configuration
        self.network = network
        
        # Establish Web3 connection
        self.w3 = self.setup_web3()
        
        # Contract storage
        self.contracts = {}
        
        # Load contract addresses
        self.load_contract_addresses()
        
        # Load contract ABIs
        self.load_contract_abis()
        
        # Initialize modules
        self.city_module = CityModule(self)
        self.company_module = CompanyModule(self)
        self.emissions_module = EmissionsModule(self)
        self.health_module = HealthModule(self)
        self.renewal_module = RenewalModule(self)

    def setup_logging(self):
        """Configure logging for the workflow"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('blockchain_workflow.log'),
                logging.StreamHandler()
            ]
        )

    def setup_web3(self):
        """
        Setup Web3 connection based on network
        
        :return: Web3 connection instance
        """
        try:
            if self.network == "development":
                return Web3(Web3.HTTPProvider('http://127.0.0.1:7545'))
            elif self.network == "sepolia":
                infura_url = f"https://sepolia.infura.io/v3/{os.getenv('INFURA_PROJECT_ID')}"
                return Web3(Web3.HTTPProvider(infura_url))
            else:
                raise ValueError(f"Unsupported network: {self.network}")
        except Exception as e:
            logging.error(f"Error setting up Web3 connection: {str(e)}")
            raise

    def load_contract_addresses(self):
        """
        Load deployed contract addresses from Truffle artifacts
        """
        try:
            network_id = self.w3.net.version
            contracts_dir = Path('./build/contracts')
            
            # Predefined contract names to load
            contract_names = [
                'CityRegister', 'CompanyRegister', 'CityEmissionsContract', 
                'RenewalTheoryContract', 'CityHealthCalculator',
                'TemperatureRenewalContract', 'MitigationContract'
            ]

            self.contract_addresses = {}
            for name in contract_names:
                filepath = contracts_dir / f'{name}.json'
                if filepath.exists():
                    with open(filepath, 'r') as f:
                        contract_data = json.load(f)
                        if network_id in contract_data.get('networks', {}):
                            self.contract_addresses[name] = contract_data['networks'][network_id]['address']
                        else:
                            logging.warning(f"No address found for {name} on network {network_id}")
                else:
                    logging.warning(f"Contract file not found: {filepath}")
        except Exception as e:
            logging.error(f"Error loading contract addresses: {str(e)}")
            raise

    def load_contract_abis(self):
        """
        Load contract ABIs and create contract instances
        """
        try:
            contracts_dir = Path('./build/contracts')
            
            for name, address in self.contract_addresses.items():
                filepath = contracts_dir / f'{name}.json'
                if filepath.exists():
                    with open(filepath, 'r') as f:
                        contract_data = json.load(f)
                        contract = self.w3.eth.contract(
                            address=address,
                            abi=contract_data['abi']
                        )
                        self.contracts[name] = contract
                else:
                    logging.warning(f"ABI file not found for {name}")
        except Exception as e:
            logging.error(f"Error loading contract ABIs: {str(e)}")
            raise

    async def load_contract(self, contract_name, abi_path):
        """
        Load specific contract instance using ABI path
        
        :param contract_name: Name of the contract
        :param abi_path: Path to the contract ABI JSON file
        :return: Loaded contract instance
        """
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

    def log_to_file(self, filename, data, receipt):
        """
        Log transaction details to a file
        
        :param filename: Log file name
        :param data: Transaction data
        :param receipt: Transaction receipt
        """
        try:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'data': data,
                'gas_used': receipt.get('gasUsed', 'N/A'),
                'transaction_hash': receipt.get('transactionHash', 'N/A').hex() if receipt.get('transactionHash') else 'N/A'
            }
            
            with open(filename, 'a') as f:
                json.dump(log_entry, f)
                f.write('\n')
        except Exception as e:
            logging.error(f"Error logging to file {filename}: {str(e)}")

    def generate_summary_report(self):
        """
        Generate a summary report of the workflow
        
        :return: Dictionary containing workflow summary
        """
        try:
            # This is a placeholder - implement actual report generation logic
            return {
                'status': 'Completed',
                'contracts_loaded': list(self.contracts.keys()),
                'network': self.network
            }
        except Exception as e:
            logging.error(f"Error generating summary report: {str(e)}")
            return {'status': 'Failed', 'error': str(e)}

    async def run_complete_workflow(self, city_data_path, company_data_path=None):
        """
        Execute complete blockchain workflow
        
        :param city_data_path: Path to city emissions data CSV
        :param company_data_path: Optional path to company data CSV
        """
        try:
            # Validate Web3 connection
            if not self.w3.is_connected():
                raise ConnectionError("Web3 connection failed")

            # Load data
            city_data = pd.read_csv(city_data_path).to_dict('records')
            
            # Optional: Load company data if path provided
            company_data = pd.read_csv(company_data_path).to_dict('records') if company_data_path else []

            # Workflow steps
            logging.info("Starting blockchain workflow...")

            # Register city data
            await self.city_module.register_city_data(city_data)
            logging.info("City data registration completed")

            # Register company data if available
            if company_data:
                await self.company_module.register_company_data(company_data)
                logging.info("Company data registration completed")

            # Process emissions
            await self.emissions_module.process_emissions_data(city_data)
            logging.info("Emissions processing completed")

            # Calculate city health
            await self.health_module.calculate_city_health(city_data)
            logging.info("City health calculation completed")

            # Calculate renewal metrics
            await self.renewal_module.calculate_renewal_metrics(city_data, company_data)
            logging.info("Renewal metrics calculation completed")

            # Generate summary report
            report = self.generate_summary_report()

            logging.info("Complete workflow executed successfully")
            return report

        except Exception as e:
            logging.error(f"Error in workflow execution: {str(e)}")
            raise

async def main():
    """
    Main execution method for the blockchain workflow
    """
    try:
        # Initialize workflow
        workflow = BlockchainWorkflow(network="development")
        
        # Run workflow with comprehensive carbon emissions data
        result = await workflow.run_complete_workflow(
            'data/carbonmonitor-cities_datas_2025-01-13.csv'
        )
        
        print("Workflow completed successfully")
        print("Summary Report:", result)
    
    except Exception as e:
        print(f"Workflow execution failed: {str(e)}")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())