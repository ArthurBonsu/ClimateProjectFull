import logging
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
from typing import List, Dict, Any

class HealthModule:
    def __init__(self, workflow, config=None):
        """
        Initialize HealthModule with workflow context and configuration
        
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
            'health_metrics': {
                'total_emissions': 'sum',
                'variance': 'var',
                'peak_emission': 'max',
                'emission_trend': 'linear_regression'
            },
            'logging': {
                'level': logging.INFO,
                'filename': 'health_module.log'
            }
        }

    def _setup_logger(self) -> logging.Logger:
        """
        Set up a specialized logger for the module
        
        :return: Configured logger
        """
        logger = logging.getLogger('HealthModule')
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

    def calculate_health_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate comprehensive health metrics
        
        :param df: Input DataFrame
        :return: DataFrame with health metrics
        """
        health_metrics = self.config['health_metrics']
        health_results = []

        for city in df['city'].unique():
            city_data = df[df['city'] == city]
            
            # Basic metrics
            metrics = {
                'city': city,
                'total_emissions': city_data['value'].sum() if health_metrics.get('total_emissions') else None,
                'variance': city_data['value'].var() if health_metrics.get('variance') else None,
                'peak_emission': city_data['value'].max() if health_metrics.get('peak_emission') else None
            }

            # Advanced trend analysis (linear regression)
            if health_metrics.get('emission_trend') == 'linear_regression':
                try:
                    city_data['date_numeric'] = pd.to_datetime(city_data['date']).astype(int) // 10**9
                    from sklearn.linear_model import LinearRegression
                    
                    X = city_data['date_numeric'].values.reshape(-1, 1)
                    y = city_data['value'].values
                    
                    model = LinearRegression()
                    model.fit(X, y)
                    
                    metrics['emission_trend_slope'] = model.coef_[0]
                    metrics['emission_trend_intercept'] = model.intercept_
                except Exception as trend_error:
                    self.logger.warning(f"Trend analysis failed for {city}: {trend_error}")
                    metrics['emission_trend_slope'] = None
                    metrics['emission_trend_intercept'] = None

            health_results.append(metrics)

        return pd.DataFrame(health_results)

    async def _calculate_city_health_batch(self, batch: pd.DataFrame):
        """
        Calculate health for a batch of cities with rate limiting
        
        :param batch: DataFrame containing city health data batch
        """
        async with self.transaction_semaphore:
            try:
                contract = self.workflow.contracts['CityHealthCalculator']
                
                for _, row in batch.iterrows():
                    try:
                        tx_hash = await contract.functions.calculateCityHealth(
                            row['city'], 
                            float(row['total_emissions'] or 0), 
                            float(row['variance'] or 0), 
                            float(row['peak_emission'] or 0)
                        ).transact({
                            'from': self.workflow.w3.eth.accounts[0],
                            'gas': 2000000
                        })
                        
                        receipt = await self.workflow.w3.eth.wait_for_transaction_receipt(tx_hash)
                        
                        # Log additional trend information if available
                        additional_data = {
                            'city': row['city'],
                            'total_emissions': row['total_emissions'],
                            'variance': row['variance'],
                            'peak_emission': row['peak_emission'],
                            'emission_trend_slope': row.get('emission_trend_slope'),
                            'emission_trend_intercept': row.get('emission_trend_intercept')
                        }
                        
                        self.workflow.log_to_file('city_health_logs.json', additional_data, receipt)
                        self.logger.info(f"Calculated health metrics for {row['city']}")
                    
                    except Exception as record_error:
                        self.logger.error(f"Error calculating health for record: {record_error}")
                        continue
            
            except Exception as batch_error:
                self.logger.error(f"Batch health calculation error: {batch_error}")
                raise

    async def calculate_city_health(self, city_data):
        """
        Calculate and register city health metrics on the blockchain
        
        :param city_data: City emissions data
        """
        try:
            # Validate contract availability
            if 'CityHealthCalculator' not in self.workflow.contracts:
                raise ValueError("CityHealthCalculator contract not loaded")
            
            # Convert to DataFrame if not already
            if not isinstance(city_data, pd.DataFrame):
                city_data = pd.DataFrame(city_data)
            
            # Validate data
            validated_data = self.validate_data(city_data)
            
            # Calculate comprehensive health metrics
            health_metrics = self.calculate_health_metrics(validated_data)
            
            self.logger.info(f"Calculating health metrics for {len(health_metrics)} cities")
            
            # Batch processing
            batch_size = self.config.get('batch_size', 100)
            batches = [
                health_metrics[i:i+batch_size] 
                for i in range(0, len(health_metrics), batch_size)
            ]
            
            # Process batches concurrently
            tasks = [self._calculate_city_health_batch(batch) for batch in batches]
            await asyncio.gather(*tasks)
            
            self.logger.info("City health calculation completed")
            
            return health_metrics
        
        except Exception as e:
            self.logger.error(f"Comprehensive city health calculation error: {e}")
            raise

    def generate_health_report(self, health_metrics: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate a comprehensive health report
        
        :param health_metrics: DataFrame with health metrics
        :return: Summary report dictionary
        """
        try:
            report = {
                'total_cities_analyzed': len(health_metrics),
                'global_total_emissions': health_metrics['total_emissions'].sum(),
                'global_average_emissions': health_metrics['total_emissions'].mean(),
                'cities_with_increasing_trend': len(
                    health_metrics[health_metrics['emission_trend_slope'] > 0]
                ),
                'cities_with_decreasing_trend': len(
                    health_metrics[health_metrics['emission_trend_slope'] < 0]
                ),
                'highest_emission_city': health_metrics.loc[
                    health_metrics['total_emissions'].idxmax()
                ]['city'],
                'lowest_emission_city': health_metrics.loc[
                    health_metrics['total_emissions'].idxmin()
                ]['city']
            }
            
            self.logger.info("Generated comprehensive health report")
            return report
        
        except Exception as e:
            self.logger.error(f"Error generating health report: {e}")
            return {}

# Optional: Configuration customization example
def create_health_module(workflow):
    """
    Factory method to create HealthModule with custom configuration
    
    :param workflow: BlockchainWorkflow instance
    :return: Configured HealthModule instance
    """
    custom_config = {
        'max_concurrent_transactions': 3,
        'batch_size': 50,
        'validation_rules': {
            'required_columns': ['city', 'date', 'sector', 'value'],
            'value_range': {'min': 0, 'max': 50},
            'date_format': '%d/%m/%Y'
        },
        'health_metrics': {
            'total_emissions': 'sum',
            'variance': 'var',
            'peak_emission': 'max',
            'emission_trend': 'linear_regression'
        }
    }
    return HealthModule(workflow, config=custom_config)