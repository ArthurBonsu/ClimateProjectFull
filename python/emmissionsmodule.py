import logging
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
from typing import List, Dict, Any

class EmissionsModule:
    def __init__(self, workflow, config=None):
        """
        Initialize EmissionsModule with workflow context and configuration
        
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
            'aggregation_strategy': {
                'method': 'sum',  # or 'mean', 'max', etc.
                'groupby_columns': ['city', 'date', 'sector']
            },
            'logging': {
                'level': logging.INFO,
                'filename': 'emissions_module.log'
            }
        }

    def _setup_logger(self) -> logging.Logger:
        """
        Set up a specialized logger for the module
        
        :return: Configured logger
        """
        logger = logging.getLogger('EmissionsModule')
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

    def aggregate_emissions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate emissions based on configuration
        
        :param df: Input DataFrame
        :return: Aggregated DataFrame
        """
        agg_strategy = self.config['aggregation_strategy']
        
        # Perform aggregation
        if agg_strategy['method'] == 'sum':
            aggregated = df.groupby(agg_strategy['groupby_columns'])['value'].sum().reset_index()
        elif agg_strategy['method'] == 'mean':
            aggregated = df.groupby(agg_strategy['groupby_columns'])['value'].mean().reset_index()
        elif agg_strategy['method'] == 'max':
            aggregated = df.groupby(agg_strategy['groupby_columns'])['value'].max().reset_index()
        else:
            raise ValueError(f"Unsupported aggregation method: {agg_strategy['method']}")
        
        self.logger.info(f"Aggregated emissions by {agg_strategy['method']} method")
        return aggregated

    async def _process_emissions_batch(self, batch: pd.DataFrame):
        """
        Process a batch of emissions data with rate limiting and error handling
        
        :param batch: DataFrame containing emissions data batch
        """
        async with self.transaction_semaphore:
            try:
                contract = self.workflow.contracts['CityEmissionsContract']
                
                for _, row in batch.iterrows():
                    try:
                        tx_hash = await contract.functions.processEmissions(
                            row['city'], 
                            row['date'], 
                            float(row['value'])
                        ).transact({
                            'from': self.workflow.w3.eth.accounts[0],
                            'gas': 2000000
                        })
                        
                        receipt = await self.workflow.w3.eth.wait_for_transaction_receipt(tx_hash)
                        
                        self.workflow.log_to_file('emissions_processing_logs.json', row.to_dict(), receipt)
                        self.logger.info(f"Processed emissions for {row['city']} on {row['date']}")
                    
                    except Exception as record_error:
                        self.logger.error(f"Error processing emissions record: {record_error}")
                        # Optionally, log failed record for later review
                        continue
            
            except Exception as batch_error:
                self.logger.error(f"Batch emissions processing error: {batch_error}")
                raise

    async def process_emissions_data(self, city_data):
        """
        Process emissions data on the blockchain with advanced processing
        
        :param city_data: Emissions data
        """
        try:
            # Validate contract availability
            if 'CityEmissionsContract' not in self.workflow.contracts:
                raise ValueError("CityEmissionsContract not loaded")
            
            # Convert to DataFrame if not already
            if not isinstance(city_data, pd.DataFrame):
                city_data = pd.DataFrame(city_data)
            
            # Validate data
            validated_data = self.validate_data(city_data)
            
            # Aggregate emissions
            aggregated_data = self.aggregate_emissions(validated_data)
            
            self.logger.info(f"Processing {len(aggregated_data)} aggregated emissions records")
            
            # Batch processing
            batch_size = self.config.get('batch_size', 100)
            batches = [
                aggregated_data[i:i+batch_size] 
                for i in range(0, len(aggregated_data), batch_size)
            ]
            
            # Process batches concurrently
            tasks = [self._process_emissions_batch(batch) for batch in batches]
            await asyncio.gather(*tasks)
            
            self.logger.info("Emissions data processing completed")
        
        except Exception as e:
            self.logger.error(f"Comprehensive emissions data processing error: {e}")
            raise

# Optional: Configuration customization example
def create_emissions_module(workflow):
    """
    Factory method to create EmissionsModule with custom configuration
    
    :param workflow: BlockchainWorkflow instance
    :return: Configured EmissionsModule instance
    """
    custom_config = {
        'max_concurrent_transactions': 3,
        'batch_size': 50,
        'validation_rules': {
            'required_columns': ['city', 'date', 'sector', 'value'],
            'value_range': {'min': 0, 'max': 50},
            'date_format': '%d/%m/%Y'
        },
        'aggregation_strategy': {
            'method': 'mean',
            'groupby_columns': ['city', 'sector']
        }
    }
    return EmissionsModule(workflow, config=custom_config)