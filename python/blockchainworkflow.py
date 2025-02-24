import os
import sys
import json
import logging
import asyncio
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Union
from web3 import Web3
from web3.exceptions import ContractLogicError

class BlockchainWorkflow:
    def __init__(
        self, 
        provider_url='http://127.0.0.1:7545',
        project_root=None,
        addresses_config=None,
        contracts_dir=None
    ):
        """
        Initialize BlockchainWorkflow with comprehensive contract loading and orchestration
        """
        # Set project root
        self.project_root = project_root or os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..')
        )
        
        # Define contract directories
        self.contracts_dir = contracts_dir or os.path.join(self.project_root, 'build', 'contracts')
        
        # Setup logging first
        self.logger = self._setup_logger()
        
        # Load contract addresses
        self.CONTRACT_ADDRESSES = self._load_contract_addresses(addresses_config)
        
        # Initialize Web3 connection
        self.w3 = Web3(Web3.HTTPProvider(provider_url))
        
        # Verify connection
        if not self.w3.is_connected():
            raise ConnectionError("Failed to connect to Ethereum provider")
        
        # Initialize contracts
        self.contracts = {}
        self._load_contracts()
        
        # Initialize modules after everything else is set up
        self._init_climate_modules()

    def _init_climate_modules(self):
        """
        Initialize climate-related modules
        """
        # Import modules here to avoid circular imports
        from climatemodule import CityModule
        from companymodule import CompanyModule
        from emmissionsmodule import EmissionsModule
        from healthmodule import HealthModule
        from renewalmodule import RenewalModule

        try:
            self.city_module = CityModule(workflow=self)
            self.company_module = CompanyModule(workflow=self)
            self.emissions_module = EmissionsModule(workflow=self)
            self.health_module = HealthModule(workflow=self)
            self.renewal_module = RenewalModule(workflow=self)
        except Exception as e:
            self.logger.error(f"Error initializing modules: {e}")
            raise

    def _load_sample_data(self):
        """
        Load sample data for testing
        
        :return: Tuple of city and company DataFrames
        """
        # Sample city data
        city_data = pd.DataFrame({
            'city': ['CityA', 'CityA', 'CityB', 'CityB'],
            'date': pd.to_datetime(['2023-01-01', '2023-02-01', '2023-01-01', '2023-02-01']),
            'sector': ['Energy', 'Transport', 'Energy', 'Transport'],
            'value': [10.5, 12.3, 8.7, 9.2]
        })
        
        # Sample company data
        company_data = pd.DataFrame({
            'company_name': ['CompanyX', 'CompanyY'],
            'registration_date': pd.to_datetime(['2023-01-01', '2023-02-01']),
            'sector': ['Energy', 'Transport'],
            'emissions_baseline': [15.0, 12.5]
        })
        
        return city_data, company_data

    async def run_full_workflow(self):
        """
        Execute full climate workflow
        
        :return: Dictionary of workflow results
        """
        try:
            # Load sample data
            city_data, company_data = self._load_sample_data()
            
            self.logger.info("Starting Climate Workflow")
            
            # 1. City Registration
            self.logger.info("Registering City Data")
            await self.city_module.register_city_data(city_data)
            
            # 2. Company Registration
            self.logger.info("Registering Company Data")
            await self.company_module.register_company_data(company_data.to_dict('records'))
            
            # 3. Emissions Processing
            self.logger.info("Processing Emissions Data")
            emissions_metrics = await self.emissions_module.process_emissions_data(city_data)
            
            # 4. Health Metrics Calculation
            self.logger.info("Calculating City Health Metrics")
            health_metrics = await self.health_module.calculate_city_health(city_data)
            
            # 5. Renewal Metrics Calculation
            self.logger.info("Calculating Renewal Metrics")
            renewal_metrics = await self.renewal_module.calculate_renewal_metrics_workflow(
                city_data, 
                company_data
            )
            
            # 6. Generate Reports
            self.logger.info("Generating Reports")
            emissions_report = self.emissions_module.generate_emissions_report(emissions_metrics)
            health_report = self.health_module.generate_health_report(health_metrics)
            renewal_report = self.renewal_module.generate_renewal_report(renewal_metrics)
            
            # Log and return results
            self.logger.info("Climate Workflow Completed Successfully")
            
            return {
                'emissions_metrics': emissions_metrics,
                'health_metrics': health_metrics,
                'renewal_metrics': renewal_metrics,
                'emissions_report': emissions_report,
                'health_report': health_report,
                'renewal_report': renewal_report
            }
        
        except Exception as e:
            self.logger.error(f"Climate Workflow Error: {e}")
            raise

    def _setup_logger(self):
        """
        Configure logging for the workflow
        
        :return: Configured logger
        """
        # Create logger
        logger = logging.getLogger('BlockchainWorkflow')
        logger.setLevel(logging.INFO)
        
        # Create log directory if it doesn't exist
        log_dir = os.path.join(self.project_root, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # File handler
        file_handler = logging.FileHandler(
            os.path.join(log_dir, 'blockchain_workflow.log'),
            encoding='utf-8'
        )
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '%(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(console_handler)
        
        return logger

    def _load_contract_addresses(self, addresses_config=None) -> Dict[str, str]:
        """
        Load contract addresses from configuration
        
        :param addresses_config: Optional path to addresses config
        :return: Dictionary of contract addresses
        """
        # Default addresses configuration path
        if not addresses_config:
            addresses_config = os.path.join(
                self.project_root, 'config', 'contract_addresses.json'
            )
        
        try:
            with open(addresses_config, 'r') as f:
                # Extract addresses, handling both string and dict formats
                config = json.load(f)
                return {
                    key: (addr['address'] if isinstance(addr, dict) else addr)
                    for key, addr in config.items()
                }
        except FileNotFoundError:
            self.logger.warning("Contract addresses configuration not found. Using default.")
            return {
                'CityRegister': '0x17e9ddb311061ba9FA3a6ea517A934cc0D136f27',
                'CompanyRegister': '0x81aDCd0724dA5Da4b796d51c09123B53A4705D3F',
                'CityEmissionsContract': '0x723332E981Ddd2577954c0e15998e66A4929b1E8',
                'RenewalTheoryContract': '0x710CcD32bbD9b108ef3FdE8178F0E5dB94DCb478',
                'CarbonCreditMarket': '0x16F6278FBae0Fa873366628118425c34Bbb1C8c0',
                'CityHealthCalculator': '0xa87d6005E919f04324E7E351fa7d988E28C1Ef03',
                'TemperatureRenewalContract': '0xC2E69562926Ad558D982DD09238d64a60D515F48',
                'ClimateReduction': '0x3B076CD48b43d99A30C7857b37704C314C0e7171',
                'MitigationContract': '0xD78652eEe39D0bF625340a3CA0cE5696A7625d15'
            }
        except json.JSONDecodeError:
            self.logger.error("Invalid contract addresses configuration.")
            raise

    def _validate_contract_abi(self, contract_data: Dict) -> bool:
        """
        Validate contract ABI structure
        
        :param contract_data: Contract JSON data
        :return: Boolean indicating ABI validity
        """
        required_keys = ['abi', 'contractName']
        return all(key in contract_data for key in required_keys) and \
               isinstance(contract_data['abi'], list)

    def _load_contracts(self):
        """
        Load contract ABIs and create contract instances with enhanced validation
        """
        for contract_name, address in self.CONTRACT_ADDRESSES.items():
            try:
                # Construct path to contract JSON
                contract_path = os.path.join(self.contracts_dir, f'{contract_name}.json')
                
                # Load contract artifact
                with open(contract_path, 'r') as f:
                    contract_data = json.load(f)
                
                # Validate ABI
                if not self._validate_contract_abi(contract_data):
                    self.logger.warning(f"Invalid ABI for {contract_name}")
                    continue
                
                # Create contract instance
                contract_instance = self.w3.eth.contract(
                    address=address,
                    abi=contract_data['abi']
                )
                
                # Store contract instance
                self.contracts[contract_name] = contract_instance
                
                self.logger.info(f"Loaded {contract_name} at {address}")
            
            except FileNotFoundError:
                self.logger.warning(f"Contract artifact not found for {contract_name}")
            except json.JSONDecodeError:
                self.logger.error(f"Invalid JSON for {contract_name}")
            except Exception as e:
                self.logger.error(f"Error loading {contract_name}: {e}")

    def get_contract(self, contract_name: str):
        """
        Retrieve a specific contract instance
        
        :param contract_name: Name of the contract
        :return: Contract instance or None
        """
        return self.contracts.get(contract_name)

    def log_to_file(self, filename, data, receipt=None):
        """
        Log transaction data to a JSON file with enhanced logging
        
        :param filename: Log file name
        :param data: Data to log
        :param receipt: Optional transaction receipt
        """
        try:
            log_dir = os.path.join(self.project_root, 'logs')
            os.makedirs(log_dir, exist_ok=True)
            
            log_path = os.path.join(log_dir, filename)
            
            # Read existing logs or create new list
            try:
                with open(log_path, 'r') as f:
                    logs = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                logs = []
            
            # Prepare log entry
            log_entry = {
                'timestamp': str(datetime.now()),
                'data': data
            }
            
            # Add transaction receipt if provided
            if receipt:
                log_entry['transaction_hash'] = receipt.transactionHash.hex()
                log_entry['gas_used'] = receipt.gasUsed
                log_entry['block_number'] = receipt.blockNumber
            
            logs.append(log_entry)
            
            # Write updated logs
            with open(log_path, 'w') as f:
                json.dump(logs, f, indent=2)
        
        except Exception as e:
            self.logger.error(f"Error logging to {filename}: {e}")

def main():
    try:
        # Add the project root to Python path to ensure module imports work
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        sys.path.insert(0, project_root)
        
        # Import workflow class
        from python.blockchainworkflow import BlockchainWorkflow
        
        # Initialize workflow with project root
        workflow = BlockchainWorkflow(
            project_root=project_root,
            provider_url='http://127.0.0.1:7545',  # Ensure this matches your local blockchain provider
            addresses_config=os.path.join(project_root, 'config', 'contract_addresses.json')
        )
        
        # Run the workflow asynchronously
        async def run_workflow():
            try:
                # Run full workflow
                results = await workflow.run_full_workflow()
                
                # Print out detailed results
                print("\n===== Climate Workflow Results =====")
                
                # Emissions Metrics
                print("\n--- Emissions Metrics ---")
                print(results.get('emissions_metrics', 'No emissions metrics available'))
                
                # Emissions Report
                print("\n--- Emissions Report ---")
                print(results.get('emissions_report', 'No emissions report available'))
                
                # Health Metrics
                print("\n--- Health Metrics ---")
                print(results.get('health_metrics', 'No health metrics available'))
                
                # Health Report
                print("\n--- Health Report ---")
                print(results.get('health_report', 'No health report available'))
                
                # Renewal Metrics
                print("\n--- Renewal Metrics ---")
                print(results.get('renewal_metrics', 'No renewal metrics available'))
                
                # Renewal Report
                print("\n--- Renewal Report ---")
                print(results.get('renewal_report', 'No renewal report available'))
                
            except Exception as workflow_error:
                print(f"Workflow Execution Error: {workflow_error}")
                # Log the full traceback
                import traceback
                traceback.print_exc()
        
        # Run the async workflow
        asyncio.run(run_workflow())
    
    except Exception as init_error:
        print(f"Workflow Initialization Error: {init_error}")
        # Log the full traceback
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()