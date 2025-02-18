import logging
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
from typing import List, Dict, Any

class RenewalModule:
    def __init__(self, workflow, config=None):
        """
        Initialize RenewalModule with workflow context and configuration
        
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
            'renewal_metrics': {
                'emissions_reduction_target': 0.1,  # 10% reduction target
                'time_window_days': 365  # Annual analysis
            },
            'logging': {
                'level': logging.INFO,
                'filename': 'renewal_module.log'
            }
        }

    def _setup_logger(self) -> logging.Logger:
        """
        Set up a specialized logger for the module
        
        :return: Configured logger
        """
        logger = logging.getLogger('RenewalModule')
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

    def calculate_renewal_metrics(self, city_df: pd.DataFrame, company_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Calculate comprehensive renewal metrics
        
        :param city_df: City emissions DataFrame
        :param company_df: Optional company emissions DataFrame
        :return: DataFrame with renewal metrics
        """
        renewal_metrics = []
        renewal_config = self.config['renewal_metrics']

        # Validate and prepare data
        city_df = self.validate_data(city_df)
        
        # Prepare company data if provided
        if company_df is not None:
            company_df = self.validate_data(company_df)
        else:
            company_df = pd.DataFrame(columns=['city', 'sector', 'emissions_baseline'])

        for city in city_df['city'].unique():
            try:
                # City-specific emissions
                city_emissions = city_df[city_df['city'] == city]
                
                # Total city emissions
                total_city_emissions = city_emissions['value'].sum()
                
                # Company emissions for the city (if available)
                company_emissions = company_df[company_df['city'] == city]['emissions_baseline'].sum() if not company_df.empty else 0
                
                # Trend analysis
                total_emissions_by_sector = city_emissions.groupby('sector')['value'].sum()
                
                # Renewal potential calculation
                renewal_potential = self._calculate_renewal_potential(
                    total_city_emissions, 
                    total_emissions_by_sector
                )
                
                # Construct metrics dictionary
                city_metrics = {
                    'city': city,
                    'total_city_emissions': total_city_emissions,
                    'company_emissions': company_emissions,
                    'emissions_by_sector': total_emissions_by_sector.to_dict(),
                    'renewal_potential': renewal_potential
                }
                
                renewal_metrics.append(city_metrics)
            
            except Exception as city_error:
                self.logger.error(f"Error calculating renewal metrics for {city}: {city_error}")
                continue
        
        return pd.DataFrame(renewal_metrics)

    def _calculate_renewal_potential(self, total_emissions: float, emissions_by_sector: pd.Series) -> Dict[str, float]:
        """
        Calculate renewal potential based on emissions
        
        :param total_emissions: Total city emissions
        :param emissions_by_sector: Emissions grouped by sector
        :return: Renewal potential metrics
        """
        renewal_config = self.config['renewal_metrics']
        
        return {
            'total_reduction_target': total_emissions * renewal_config['emissions_reduction_target'],
            'sector_reduction_potential': {
                sector: emissions * renewal_config['emissions_reduction_target']
                for sector, emissions in emissions_by_sector.items()
            }
        }

    async def _calculate_renewal_metrics_batch(self, batch: pd.DataFrame):
        """
        Calculate renewal metrics for a batch of cities
        
        :param batch: DataFrame containing renewal metrics batch
        """
        async with self.transaction_semaphore:
            try:
                contract = self.workflow.contracts['RenewalTheoryContract']
                
                for _, row in batch.iterrows():
                    try:
                        tx_hash = await contract.functions.calculateRenewalMetrics(
                            row['city'], 
                            float(row['total_city_emissions']), 
                            float(row['company_emissions'])
                        ).transact({
                            'from': self.workflow.w3.eth.accounts[0],
                            'gas': 2000000
                        })
                        
                        receipt = await self.workflow.w3.eth.wait_for_transaction_receipt(tx_hash)
                        
                        # Log detailed renewal metrics
                        additional_data = {
                            'city': row['city'],
                            'total_city_emissions': row['total_city_emissions'],
                            'company_emissions': row['company_emissions'],
                            'renewal_potential': row.get('renewal_potential', {})
                        }
                        
                        self.workflow.log_to_file('renewal_metrics_logs.json', additional_data, receipt)
                        self.logger.info(f"Calculated renewal metrics for {row['city']}")
                    
                    except Exception as record_error:
                        self.logger.error(f"Error calculating renewal metrics for record: {record_error}")
                        continue
            
            except Exception as batch_error:
                self.logger.error(f"Batch renewal metrics calculation error: {batch_error}")
                raise

    async def calculate_renewal_metrics_workflow(self, city_data, company_data=None):
        """
        Main workflow for calculating and registering renewal metrics
        
        :param city_data: City emissions data
        :param company_data: Optional company emissions data
        """
        try:
            # Validate contract availability
            if 'RenewalTheoryContract' not in self.workflow.contracts:
                raise ValueError("RenewalTheoryContract not loaded")
            
            # Convert to DataFrame if not already
            if not isinstance(city_data, pd.DataFrame):
                city_data = pd.DataFrame(city_data)
            
            # Optional company data
            if company_data and not isinstance(company_data, pd.DataFrame):
                company_data = pd.DataFrame(company_data)
            
            # Calculate comprehensive renewal metrics
            renewal_metrics = self.calculate_renewal_metrics(city_data, company_data)
            
            self.logger.info(f"Calculating renewal metrics for {len(renewal_metrics)} cities")
            
            # Batch processing
            batch_size = self.config.get('batch_size', 100)
            batches = [
                renewal_metrics[i:i+batch_size] 
                for i in range(0, len(renewal_metrics), batch_size)
            ]
            
            # Process batches concurrently
            tasks = [self._calculate_renewal_metrics_batch(batch) for batch in batches]
            await asyncio.gather(*tasks)
            
            self.logger.info("Renewal metrics calculation completed")
            
            return renewal_metrics
        
        except Exception as e:
            self.logger.error(f"Comprehensive renewal metrics calculation error: {e}")
            raise

# Optional: Configuration customization example
def create_renewal_module(workflow):
    #"""
   # Factory method to create RenewalModule with custom configuration
    
   # :param workflow: BlockchainWorkflow instance
   # :return: Configured RenewalModule instance