import os
import json
import logging
import asyncio
from pathlib import Path
from typing import Dict, Optional, Union
from web3 import Web3
from web3.exceptions import ContractLogicError

class EnhancedBlockchainDeployment:
    """Enhanced blockchain deployment handler with multi-environment support"""
    
    # Supported environments and their configurations
    ENVIRONMENTS = {
        'development': {
            'providers': [
                'http://127.0.0.1:7545',  # Ganache UI
                'http://localhost:7545',
                'http://127.0.0.1:8545',  # Hardhat
                'http://localhost:8545',
                'http://127.0.0.1:9545',  # Truffle
                'http://localhost:9545'
            ],
            'gas_limit': 6000000,
            'gas_price_multiplier': 1.5
        },
        'testnet': {
            'providers': [
                'https://data-seed-prebsc-1-s1.binance.org:8545/',  # BSC Testnet
                'https://sepolia.infura.io/v3/YOUR-PROJECT-ID',     # Ethereum Sepolia
                'https://polygon-mumbai.infura.io/v3/YOUR-PROJECT-ID'  # Polygon Mumbai
            ],
            'gas_limit': 8000000,
            'gas_price_multiplier': 2
        },
        'mainnet': {
            'providers': [
                'https://bsc-dataseed.binance.org/',              # BSC
                'https://mainnet.infura.io/v3/YOUR-PROJECT-ID',   # Ethereum
                'https://polygon-rpc.com'                         # Polygon
            ],
            'gas_limit': 10000000,
            'gas_price_multiplier': 1.2
        }
    }

    def __init__(self, 
                 environment: str = "development",
                 custom_provider: Optional[str] = None,
                 project_root: Optional[str] = None):
        """
        Initialize the enhanced blockchain deployment handler
        
        Args:
            environment: Target environment (development/testnet/mainnet)
            custom_provider: Optional custom Web3 provider URL
            project_root: Optional project root directory
        """
        self.environment = environment.lower()
        self.custom_provider = custom_provider
        self.project_root = project_root or os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        
        # Initialize core attributes
        self.w3 = None
        self.contracts = {}
        self.contract_addresses = {}
        self.deployer_account = None
        
        # Setup logging
        self._setup_logging()
        
        # Environment validation
        if self.environment not in self.ENVIRONMENTS and not custom_provider:
            raise ValueError(f"Unsupported environment: {environment}")
            
        # Initialize Web3 connection
        self._initialize_web3()

    def _setup_logging(self):
        """Configure enhanced logging system"""
        log_dir = os.path.join(self.project_root, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(log_dir, f'blockchain_{self.environment}.log')),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('BlockchainDeployment')

    def _initialize_web3(self):
        """Initialize Web3 connection with comprehensive provider handling"""
        if self.custom_provider:
            providers = [self.custom_provider]
        else:
            providers = self.ENVIRONMENTS[self.environment]['providers']

        for provider_url in providers:
            try:
                if provider_url.startswith('http'):
                    provider = Web3.HTTPProvider(provider_url)
                elif provider_url.startswith('ws'):
                    provider = Web3.WebsocketProvider(provider_url)
                else:
                    continue

                w3 = Web3(provider)
                if w3.is_connected():
                    self.w3 = w3
                    self.deployer_account = w3.eth.accounts[0] if w3.eth.accounts else None
                    self.logger.info(f"Connected to blockchain at {provider_url}")
                    return
            except Exception as e:
                self.logger.warning(f"Failed to connect to {provider_url}: {str(e)}")

        raise ConnectionError("Failed to connect to any blockchain provider")

    def load_contract_artifacts(self, contract_name: str) -> Dict:
        """
        Load and validate contract artifacts with multiple source support
        
        Args:
            contract_name: Name of the contract to load
        
        Returns:
            Dict containing contract artifacts
        """
        # Potential artifact locations
        artifact_paths = [
            os.path.join(self.project_root, 'build', 'contracts', f'{contract_name}.json'),
            os.path.join(self.project_root, 'artifacts', contract_name, f'{contract_name}.json'),
            os.path.join(self.project_root, 'deployments', self.environment, f'{contract_name}.json')
        ]

        for path in artifact_paths:
            try:
                with open(path, 'r') as f:
                    artifact = json.load(f)
                
                # Validate artifact structure
                if not all(key in artifact for key in ['abi', 'bytecode']):
                    continue
                
                return artifact
            except Exception as e:
                self.logger.debug(f"Failed to load artifact from {path}: {str(e)}")

        raise FileNotFoundError(f"No valid artifact found for {contract_name}")

    async def deploy_contract(self, 
                            contract_name: str,
                            *constructor_args,
                            existing_address: Optional[str] = None) -> Dict:
        """
        Deploy or load a single contract with comprehensive error handling
        
        Args:
            contract_name: Name of the contract to deploy
            constructor_args: Optional constructor arguments
            existing_address: Optional existing contract address to load
        
        Returns:
            Dict containing contract instance and address
        """
        try:
            # Load contract artifacts
            contract_data = self.load_contract_artifacts(contract_name)
            
            if existing_address:
                # Load existing contract
                contract_instance = self.w3.eth.contract(
                    address=existing_address,
                    abi=contract_data['abi']
                )
                self.logger.info(f"Loaded existing {contract_name} at {existing_address}")
                
            else:
                # Deploy new contract
                Contract = self.w3.eth.contract(
                    abi=contract_data['abi'],
                    bytecode=contract_data['bytecode']
                )

                # Prepare deployment transaction
                env_config = self.ENVIRONMENTS[self.environment]
                gas_price = self.w3.eth.gas_price
                
                construct_txn = Contract.constructor(*constructor_args).build_transaction({
                    'from': self.deployer_account,
                    'gas': env_config['gas_limit'],
                    'gasPrice': int(gas_price * env_config['gas_price_multiplier']),
                    'nonce': self.w3.eth.get_transaction_count(self.deployer_account)
                })

                # Sign and send transaction
                signed_txn = self.w3.eth.account.sign_transaction(construct_txn, private_key=os.getenv('DEPLOYER_PRIVATE_KEY'))
                tx_hash = self.w3.eth.send_raw_transaction(signed_txn.rawTransaction)
                
                # Wait for deployment
                tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
                contract_instance = self.w3.eth.contract(
                    address=tx_receipt.contractAddress,
                    abi=contract_data['abi']
                )
                self.logger.info(f"Deployed {contract_name} at {tx_receipt.contractAddress}")

            # Store contract information
            self.contracts[contract_name] = contract_instance
            self.contract_addresses[contract_name] = contract_instance.address

            return {
                'contract': contract_instance,
                'address': contract_instance.address
            }

        except Exception as e:
            self.logger.error(f"Error deploying {contract_name}: {str(e)}")
            raise

    async def deploy_all_contracts(self, 
                                 deployment_order: list,
                                 existing_addresses: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """
        Deploy or load all contracts in specified order
        
        Args:
            deployment_order: List of contract names in deployment order
            existing_addresses: Optional dict of existing contract addresses
        
        Returns:
            Dict of deployed contract addresses
        """
        existing_addresses = existing_addresses or {}
        
        for contract_name in deployment_order:
            try:
                # Check if we should load existing contract
                existing_address = existing_addresses.get(contract_name)
                
                # Determine constructor arguments based on contract
                constructor_args = []
                if contract_name == 'CarbonCreditMarket':
                    constructor_args = [
                        self.contract_addresses.get('UniswapRouter'),
                        self.contract_addresses.get('CarbonToken'),
                        self.contract_addresses.get('USDToken')
                    ]
                elif contract_name == 'MitigationContract':
                    constructor_args = [
                        self.contract_addresses.get('CarbonFeed'),
                        self.contract_addresses.get('TemperatureFeed')
                    ]

                # Deploy or load contract
                await self.deploy_contract(
                    contract_name,
                    *constructor_args,
                    existing_address=existing_address
                )

            except Exception as e:
                self.logger.error(f"Failed to deploy {contract_name}: {str(e)}")
                raise

        return self.contract_addresses

    def save_deployment_info(self):
        """Save deployment information to JSON file"""
        deployment_info = {
            'environment': self.environment,
            'timestamp': str(datetime.datetime.now()),
            'addresses': self.contract_addresses
        }
        
        output_dir = os.path.join(self.project_root, 'deployments', self.environment)
        os.makedirs(output_dir, exist_ok=True)
        
        with open(os.path.join(output_dir, 'deployment.json'), 'w') as f:
            json.dump(deployment_info, f, indent=2)

    @staticmethod
    def load_existing_deployment(project_root: str, environment: str) -> Dict[str, str]:
        """
        Load existing deployment addresses
        
        Args:
            project_root: Project root directory
            environment: Target environment
        
        Returns:
            Dict of contract addresses
        """
        deployment_file = os.path.join(project_root, 'deployments', environment, 'deployment.json')
        
        try:
            with open(deployment_file, 'r') as f:
                deployment_info = json.load(f)
            return deployment_info['addresses']
        except Exception:
            return {}

async def main():
    # Example usage
    try:
        # Initialize deployment handler
        deployer = EnhancedBlockchainDeployment(
            environment='development',
            project_root='path/to/project'
        )

        # Contract deployment order
        deployment_order = [
            'CityRegister',
            'CompanyRegister',
            'CityEmissionsContract',
            'RenewalTheoryContract',
            'CityHealthCalculator',
            'TemperatureRenewalContract',
            'CarbonCreditMarket',
            'MitigationContract'
        ]

        # Try to load existing deployment
        existing_addresses = EnhancedBlockchainDeployment.load_existing_deployment(
            deployer.project_root,
            deployer.environment
        )

        # Deploy or load contracts
        deployed_addresses = await deployer.deploy_all_contracts(
            deployment_order,
            existing_addresses=existing_addresses
        )

        # Save deployment information
        deployer.save_deployment_info()

        print("\nDeployment completed successfully!")
        for name, address in deployed_addresses.items():
            print(f"{name}: {address}")

    except Exception as e:
        print(f"Deployment failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())