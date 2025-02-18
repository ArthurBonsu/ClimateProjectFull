import logging
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
from typing import List, Dict, Any

class CityModule:
    def __init__(self, workflow, config=None):
        """
        Initialize CityModule with workflow context and configuration
        
        :param workflow: BlockchainWorkflow instance
        :param config: Configuration dictionary for module behavior
        """
        self.workflow = workflow
        self.config = config or self._default_config()
        
        # Setup specialized logger
        self.logger = self._setup_logger()
        
        # Transaction rate limiting
        self.transaction_semaphore = asyncio.Semaphore(
            self.config.get('max_concurrent_transactions', 5)
        )

    def _default_config(self) -> Dict[str, Any]:
        """
        Provide default configuration for the module
        
        :return: Default configuration dictionary
        """
        return {
            'max_concurrent_transactions': 5,
            'batch_size': 100,
            'validation_rules': {
                'required_columns': ['city', 'date', 'sector', 'value'],
                'value_range': {
                    'min': 0,
                    'max': 100  # Adjust based on your emissions scale
                },
                'date_format': '%d/%m/%Y'
            },
            'logging': {
                'level': logging.INFO,
                'filename': 'city_module.log'
            }
        }

    def _setup_logger(self) -> logging.Logger:
        """
        Set up a specialized logger for the module
        
        :return: Configured logger
        """
        logger = logging.getLogger('CityModule')
        logger.setLevel(self.config['logging']['level'])
        
        # File handler
        file_handler = logging.FileHandler(
            self.config['logging']['filename'], 
            encoding='utf-8'
        )
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(file_handler)
        
        return logger

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate input data based on configuration rules
        
        :param df: Input DataFrame
        :return: Validated DataFrame
        """
        validation_rules = self.config['validation_rules']
        
        # Check required columns
        missing_columns = set(validation_rules['required_columns']) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")
        
        # Data type and value range validation
        try:
            # Convert date to specified format
            df['date'] = pd.to_datetime(
                df['date'], 
                format=validation_rules['date_format']
            ).dt.strftime('%d/%m/%Y')
            
            # Validate numeric values
            df['value'] = pd.to_numeric(df['value'], errors='raise')
            
            # Check value range
            value_range = validation_rules['value_range']
            invalid_values = df[
                (df['value'] < value_range['min']) | 
                (df['value'] > value_range['max'])
            ]
            
            if not invalid_values.empty:
                self.logger.warning(f"Found {len(invalid_values)} invalid value records")
                df = df[
                    (df['value'] >= value_range['min']) & 
                    (df['value'] <= value_range['max'])
                ]
        
        except Exception as e:
            self.logger.error(f"Data validation error: {e}")
            raise
        
        return df

    async def _register_city_batch(self, batch: pd.DataFrame):
        """
        Register a batch of city data with rate limiting and error handling
        
        :param batch: DataFrame containing city data batch
        """
        async with self.transaction_semaphore:
            try:
                contract = self.workflow.contracts['CityRegister']
                
                for _, record in batch.iterrows():
                    try:
                        tx_hash = await contract.functions.registerCity(
                            record['city'],
                            record['date'],
                            record['sector'],
                            float(record['value'])
                        ).transact({
                            'from': self.workflow.w3.eth.accounts[0],
                            'gas': 2000000
                        })
                        
                        receipt = await self.workflow.w3.eth.wait_for_transaction_receipt(tx_hash)
                        
                        self.workflow.log_to_file('city_register_logs.json', record.to_dict(), receipt)
                        self.logger.info(f"Registered city: {record['city']} on {record['date']}")
                    
                    except Exception as record_error:
                        self.logger.error(f"Error registering record: {record_error}")
                        # Optionally, log failed record for later review
                        continue
            
            except Exception as batch_error:
                self.logger.error(f"Batch registration error: {batch_error}")
                raise

    async def register_city_data(self, city_data):
    """
    Register city data on the blockchain with enhanced flexibility
    """
    try:
        if 'CityRegister' not in self.workflow.contracts:
            raise ValueError("CityRegister contract not loaded")

        contract = self.workflow.contracts['CityRegister']
        
        # Group data by unique cities to avoid redundant registrations
        unique_cities = city_data.groupby('city').first().reset_index()
        
        for _, record in unique_cities.iterrows():
            try:
                tx_hash = await contract.functions.registerCity(
                    record['city'],
                    record['date'],  # Using first date for city registration
                    record['sector'],
                    float(record['value'])
                ).transact({
                    'from': self.workflow.w3.eth.accounts[0],
                    'gas': 2000000
                })
                
                receipt = await self.workflow.w3.eth.wait_for_transaction_receipt(tx_hash)
                self.workflow.log_to_file('city_register_logs.json', record.to_dict(), receipt)
                
                logging.info(f"Registered city data for {record['city']}")
            
            except Exception as record_error:
                logging.error(f"Error registering city: {record_error}")
                continue

    except Exception as e:
        logging.error(f"Error in city data registration: {str(e)}")
        raise

# Optional: Configuration customization example
def create_city_module(workflow):
    """
    Factory method to create CityModule with custom configuration
    
    :param workflow: BlockchainWorkflow instance
    :return: Configured CityModule instance
    """
    custom_config = {
        'max_concurrent_transactions': 3,
        'batch_size': 50,
        'validation_rules': {
            'required_columns': ['city', 'date', 'sector', 'value'],
            'value_range': {'min': 0, 'max': 50},
            'date_format': '%d/%m/%Y'
        }
    }
    return CityModule(workflow, config=custom_config)