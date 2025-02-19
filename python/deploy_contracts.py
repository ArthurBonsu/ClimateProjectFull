import os
import json
import subprocess
import logging
from web3 import Web3

class ContractDeployer:
    def __init__(self, network="development"):
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Setup Web3 connection
        if network == "development":
            self.w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:7545'))
        else:
            raise ValueError(f"Unsupported network: {network}")
        
        # Project paths
        self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.contracts_build_dir = os.path.join(self.project_root, 'build', 'contracts')
        
    def run_truffle_migrate(self):
        """
        Run Truffle migration to deploy contracts
        """
        try:
            # Ensure we're in the project root
            os.chdir(self.project_root)
            
            # Run Truffle migrate
            result = subprocess.run(
                ['truffle', 'migrate', '--reset', '--network', 'development'], 
                capture_output=True, 
                text=True
            )
            
            if result.returncode != 0:
                self.logger.error("Truffle migration failed")
                self.logger.error(result.stderr)
                raise Exception("Truffle migration failed")
            
            self.logger.info("Truffle migration completed successfully")
        except Exception as e:
            self.logger.error(f"Deployment error: {e}")
            raise

    def update_contract_addresses(self):
        """
        Update contract addresses in configuration files
        """
        try:
            # Network ID for local Ganache
            network_id = self.w3.net.version
            
            # Contracts to track
            contract_names = [
                'CityRegister', 'CompanyRegister', 'CityEmissionsContract', 
                'RenewalTheoryContract', 'CityHealthCalculator',
                'TemperatureRenewalContract', 'MitigationContract'
            ]
            
            # Configuration to store addresses
            contract_addresses = {}
            
            # Read contract addresses from build artifacts
            for contract_name in contract_names:
                contract_path = os.path.join(self.contracts_build_dir, f'{contract_name}.json')
                
                if os.path.exists(contract_path):
                    with open(contract_path, 'r') as f:
                        contract_data = json.load(f)
                    
                    # Get contract address for the current network
                    if network_id in contract_data.get('networks', {}):
                        contract_addresses[contract_name] = contract_data['networks'][network_id]['address']
                        self.logger.info(f"Loaded address for {contract_name}")
                    else:
                        self.logger.warning(f"No address found for {contract_name}")
            
            # Write addresses to a configuration file
            config_path = os.path.join(self.project_root, 'config', 'contract_addresses.json')
            with open(config_path, 'w') as f:
                json.dump(contract_addresses, f, indent=2)
            
            self.logger.info(f"Contract addresses saved to {config_path}")
            
            return contract_addresses
        
        except Exception as e:
            self.logger.error(f"Error updating contract addresses: {e}")
            raise

def main():
    try:
        # Initialize deployer
        deployer = ContractDeployer()
        
        # Run Truffle migration
        deployer.run_truffle_migrate()
        
        # Update and save contract addresses
        contract_addresses = deployer.update_contract_addresses()
        
        print("Deployment completed successfully")
        print("Deployed Contract Addresses:", json.dumps(contract_addresses, indent=2))
    
    except Exception as e:
        print(f"Deployment failed: {e}")

if __name__ == "__main__":
    main()