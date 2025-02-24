import logging
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
from typing import List, Dict, Any
from sklearn.linear_model import LinearRegression

class HealthModule:
    def __init__(self, workflow=None, config=None):
        """
        Initialize HealthModule with workflow context and configuration
        
        :param workflow: BlockchainWorkflow instance
        :param config: Configuration dictionary for module behavior
        """
        # Store workflow instance
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
        Calculate comprehensive health metrics for cities
        
        :param df: Input DataFrame with city emissions data
        :return: DataFrame with health metrics
        """
        # Validate input data
        validated_df = self.validate_data(df)
        
        health_metrics = []
        
        for city in validated_df['city'].unique():
            city_data = validated_df[validated_df['city'] == city].copy()  # Create a copy
            
            # Basic metrics
            total_emissions = city_data['value'].sum()
            variance = city_data['value'].var()
            peak_emission = city_data['value'].max()
            
            # Trend analysis (linear regression)
            try:
                # Convert dates to numeric for regression
                city_data.loc[:, 'date_numeric'] = pd.to_datetime(city_data['date']).astype(int) // 10**9
                
                X = city_data['date_numeric'].values.reshape(-1, 1)
                y = city_data['value'].values
                
                model = LinearRegression()
                model.fit(X, y)
                
                trend_slope = model.coef_[0]
                trend_intercept = model.intercept_
            except Exception as trend_error:
                self.logger.warning(f"Trend analysis failed for {city}: {trend_error}")
                trend_slope = None
                trend_intercept = None
            
            # Compile metrics
            city_metrics = {
                'city': city,
                'total_emissions': total_emissions,
                'variance': variance,
                'peak_emission': peak_emission,
                'trend_slope': trend_slope,
                'trend_intercept': trend_intercept
            }
            
            health_metrics.append(city_metrics)
        
        return pd.DataFrame(health_metrics)

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
            
            # Calculate health metrics
            health_metrics = self.calculate_health_metrics(city_data)
            
            # Get the contract instance
            health_contract = self.workflow.contracts['CityHealthCalculator']
            
            # Process each city's health metrics
            for _, row in health_metrics.iterrows():
                try:
                    # Interact with contract to calculate health
                    tx_hash = await health_contract.functions.calculateCityHealth(
                        row['city'], 
                        float(row['total_emissions']), 
                        float(row['variance']), 
                        float(row['peak_emission'])
                    ).transact({
                        'from': self.workflow.w3.eth.accounts[0],
                        'gas': 2000000
                    })
                    
                    # Wait for transaction receipt
                    receipt = await self.workflow.w3.eth.wait_for_transaction_receipt(tx_hash)
                    
                    # Log the transaction
                    self.workflow.log_to_file('city_health_logs.json', row.to_dict(), receipt)
                    
                    self.logger.info(f"Processed health metrics for {row['city']}")
                
                except Exception as record_error:
                    self.logger.error(f"Error processing health metrics for {row['city']}: {record_error}")
                    continue
            
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
                    health_metrics[health_metrics['trend_slope'] > 0]
                ),
                'cities_with_decreasing_trend': len(
                    health_metrics[health_metrics['trend_slope'] < 0]
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
        }
    }
    return HealthModule(workflow, config=custom_config)

# Example usage
async def main():
    # Create workflow
    workflow = BlockchainWorkflow()
    
    # Create health module
    health_module = HealthModule(workflow)
    
    # Sample data (replace with actual data loading)
    sample_data = pd.DataFrame({
        'city': ['CityA', 'CityA', 'CityB', 'CityB'],
        'date': ['01/01/2023', '02/01/2023', '01/01/2023', '02/01/2023'],
        'sector': ['Energy', 'Energy', 'Transport', 'Transport'],
        'value': [10.5, 12.3, 8.7, 9.2]
    })
    
    # Calculate and register health metrics
    health_metrics = await health_module.calculate_city_health(sample_data)
    
    # Generate health report
    report = health_module.generate_health_report(health_metrics)
    print(report)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())