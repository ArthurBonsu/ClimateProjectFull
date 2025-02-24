import logging
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
import os
from typing import List, Dict, Any, Tuple

class CityModule:
    def __init__(self, workflow, config=None):
        """
        Initialize CityModule with workflow context and configuration
        
        :param workflow: BlockchainWorkflow instance
        :param config: Configuration dictionary for module behavior
        """
        self.workflow = workflow
        self.config = config or self._default_config()
        self.logger = self._setup_logger()
        self.data_path = os.path.join(self.workflow.project_root, 'data', 'carbonmonitor-cities_datas_2025-01-13.csv')
        self.transaction_semaphore = asyncio.Semaphore(
            self.config.get('max_concurrent_transactions', 5)
        )

    def _setup_logger(self):
        """Configure logging for the CityModule"""
        logger = logging.getLogger('CityModule')
        logger.setLevel(self.config['logging']['level'])
        
        log_dir = os.path.join(self.workflow.project_root, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, self.config['logging']['filename'])
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '%(name)s - %(levelname)s - %(message)s'
        ))
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def _default_config(self) -> Dict[str, Any]:
        """Provide default configuration for the module"""
        return {
            'max_concurrent_transactions': 5,
            'batch_size': 100,
            'validation_rules': {
                'required_columns': ['city', 'date', 'sector', 'value'],
                'value_range': {
                    'min': 0,
                    'max': 1000  # Adjusted for real emissions data
                },
                'date_format': '%Y-%m-%d'  # Updated for standard CSV format
            },
            'logging': {
                'level': logging.INFO,
                'filename': 'city_module.log'
            }
        }

    def load_and_validate_data(self) -> Tuple[pd.DataFrame, List[str]]:
        """
        Load and validate the carbon monitor cities data
        
        :return: Tuple of (validated DataFrame, list of validation messages)
        """
        validation_messages = []
        
        try:
            # Load the CSV file
            self.logger.info(f"Loading data from {self.data_path}")
            df = pd.read_csv(self.data_path)
            validation_messages.append(f"Successfully loaded {len(df)} records")

            # Basic data validation
            required_cols = self.config['validation_rules']['required_columns']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")

            # Clean and transform data
            df = self._clean_data(df)
            validation_messages.extend(self._validate_data(df))

            return df, validation_messages

        except FileNotFoundError:
            msg = f"Data file not found at {self.data_path}"
            self.logger.error(msg)
            raise FileNotFoundError(msg)
        except Exception as e:
            msg = f"Error loading data: {str(e)}"
            self.logger.error(msg)
            raise

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and prepare the data for processing
        
        :param df: Raw DataFrame
        :return: Cleaned DataFrame
        """
        # Make a copy to avoid modifying original data
        df = df.copy()
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Remove any rows with missing values
        df = df.dropna(subset=['city', 'value'])
        
        # Remove any leading/trailing whitespace
        df['city'] = df['city'].str.strip()
        if 'sector' in df.columns:
            df['sector'] = df['sector'].str.strip()
        
        # Convert values to float and handle any unit conversions if needed
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        return df

    def _validate_data(self, df: pd.DataFrame) -> List[str]:
        """
        Validate the cleaned data
        
        :param df: DataFrame to validate
        :return: List of validation messages
        """
        messages = []
        rules = self.config['validation_rules']
        
        # Check value ranges
        value_min = rules['value_range']['min']
        value_max = rules['value_range']['max']
        out_of_range = df[
            (df['value'] < value_min) | 
            (df['value'] > value_max)
        ]
        
        if not out_of_range.empty:
            messages.append(
                f"Found {len(out_of_range)} values outside expected range "
                f"({value_min}-{value_max})"
            )
            # Log the problematic values
            for _, row in out_of_range.iterrows():
                messages.append(
                    f"Unusual value for {row['city']}: {row['value']} "
                    f"on {row['date']}"
                )
        
        # Check for duplicate entries
        duplicates = df[df.duplicated(['city', 'date', 'sector'], keep=False)]
        if not duplicates.empty:
            messages.append(
                f"Found {len(duplicates)} duplicate entries"
            )
        
        # Add summary statistics
        messages.append(f"Total number of cities: {df['city'].nunique()}")
        messages.append(f"Date range: {df['date'].min()} to {df['date'].max()}")
        messages.append(f"Average emission value: {df['value'].mean():.2f}")
        
        return messages

    async def register_city_data(self, city_data=None):
        """
        Register city data on the blockchain
        
        :param city_data: Optional DataFrame to use instead of loading from file
        """
        try:
            # Load and validate data if not provided
            if city_data is None:
                city_data, validation_messages = self.load_and_validate_data()
                for msg in validation_messages:
                    self.logger.info(msg)

            # Validate contract availability
            if 'CityRegister' not in self.workflow.contracts:
                raise ValueError("CityRegister contract not loaded")

            contract = self.workflow.contracts['CityRegister']
            
            # Process unique cities
            unique_cities = city_data['city'].unique()
            
            for city in unique_cities:
                try:
                    async with self.transaction_semaphore:
                        # Prepare transaction parameters
                        tx_params = {
                            'from': self.workflow.w3.eth.accounts[0],
                            'gas': 2000000
                        }
                        
                        # Call contract method with correct string argument
                        tx_hash = contract.functions.registerCity(
                            city,  # Pass city as a string
                            datetime.now().strftime(self.config['validation_rules']['date_format']),
                            'total',  # Default sector
                            0  # Default value
                        ).transact(tx_params)
                        
                        # Wait for receipt
                        receipt = await self.workflow.w3.eth.wait_for_transaction_receipt(tx_hash)
                        
                        # Log transaction
                        self.workflow.log_to_file(
                            'city_register_logs.json',
                            {'city': city},
                            receipt
                        )
                        
                        self.logger.info(f"Registered city: {city}")
                
                except Exception as record_error:
                    self.logger.error(f"Error registering city record: {record_error}")
                    continue

        except Exception as e:
            self.logger.error(f"City data registration error: {e}")
            raise

def create_city_module(workflow, config=None):
    """Factory method to create CityModule with optional custom configuration"""
    return CityModule(workflow, config)