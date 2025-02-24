import os
import logging
import pandas as pd
from typing import Tuple, List, Dict, Any
from web3 import Web3
from datetime import datetime

class CompanyModule:
    def __init__(self, workflow):
        """
        Initialize CompanyModule with workflow context
        
        :param workflow: BlockchainWorkflow instance
        """
        self.workflow = workflow
        self.logger = logging.getLogger('CompanyModule')
        self.data_path = os.path.join(self.workflow.project_root, 'data', 'companies_data_2025-01-13.csv')
        
    def load_and_validate_data(self) -> Tuple[pd.DataFrame, List[str]]:
        """
        Load and validate the companies data
        
        :return: Tuple of (validated DataFrame, list of validation messages)
        """
        validation_messages = []
        
        try:
            # Load the CSV file
            self.logger.info(f"Loading data from {self.data_path}")
            df = pd.read_csv(self.data_path)
            validation_messages.append(f"Successfully loaded {len(df)} records")

            # Required columns
            required_cols = ['company_name', 'registration_date', 'sector', 'emissions_baseline']
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
        
        # Convert registration date to datetime
        df['registration_date'] = pd.to_datetime(df['registration_date'])
        
        # Remove any rows with missing values
        df = df.dropna(subset=['company_name', 'emissions_baseline'])
        
        # Remove any leading/trailing whitespace
        df['company_name'] = df['company_name'].str.strip()
        df['sector'] = df['sector'].str.strip()
        
        # Convert emissions baseline to float
        df['emissions_baseline'] = pd.to_numeric(df['emissions_baseline'], errors='coerce')
        
        return df

    def _validate_data(self, df: pd.DataFrame) -> List[str]:
        """
        Validate the cleaned data
        
        :param df: DataFrame to validate
        :return: List of validation messages
        """
        messages = []
        
        # Check value ranges for emissions baseline
        value_min = 0
        value_max = 1000  # Adjust as needed
        out_of_range = df[
            (df['emissions_baseline'] < value_min) | 
            (df['emissions_baseline'] > value_max)
        ]
        
        if not out_of_range.empty:
            messages.append(
                f"Found {len(out_of_range)} values outside expected range "
                f"({value_min}-{value_max})"
            )
            # Log the problematic values
            for _, row in out_of_range.iterrows():
                messages.append(
                    f"Unusual emissions baseline for {row['company_name']}: {row['emissions_baseline']}"
                )
        
        # Check for duplicate entries
        duplicates = df[df.duplicated(['company_name', 'registration_date'], keep=False)]
        if not duplicates.empty:
            messages.append(
                f"Found {len(duplicates)} duplicate entries"
            )
        
        # Add summary statistics
        messages.append(f"Total number of companies: {df['company_name'].nunique()}")
        messages.append(f"Date range: {df['registration_date'].min()} to {df['registration_date'].max()}")
        messages.append(f"Average emissions baseline: {df['emissions_baseline'].mean():.2f}")
        
        return messages

    async def register_company_data(self, company_data=None):
        """
        Register company data on the blockchain
        
        :param company_data: Optional DataFrame to use instead of loading from file
        """
        try:
            # Load and validate data if not provided
            if company_data is None:
                company_data, validation_messages = self.load_and_validate_data()
                for msg in validation_messages:
                    self.logger.info(msg)

            # Validate contract availability
            if 'CompanyRegister' not in self.workflow.contracts:
                raise ValueError("CompanyRegister contract not loaded")

            contract = self.workflow.contracts['CompanyRegister']
            
            # Convert to list of dictionaries if DataFrame
            if isinstance(company_data, pd.DataFrame):
                company_data = company_data.to_dict('records')
            
            # Process each company record
            for record in company_data:
                try:
                    # Prepare transaction parameters
                    tx_params = {
                        'from': self.workflow.w3.eth.accounts[0],
                        'gas': 2000000
                    }
                    
                    # Convert registration date to timestamp
                    registration_timestamp = int(pd.to_datetime(record['registration_date']).timestamp())
                    
                    # Prepare company arguments 
                    tx_hash = contract.functions.registerCompany(
                        self.workflow.w3.eth.accounts[0],  # Company address 
                        self.workflow.w3.eth.accounts[0],  # Owner address
                        registration_timestamp,  # Registration timestamp
                        int(record['emissions_baseline'] * 1000),  # Baseline emissions (scaled)
                        0,  # Longitude (if applicable)
                        0   # Latitude (if applicable)
                    ).transact(tx_params)
                    
                    # Wait for receipt
                    receipt = await self.workflow.w3.eth.wait_for_transaction_receipt(tx_hash)
                    
                    # Log transaction
                    self.workflow.log_to_file('company_register_logs.json', record, receipt)
                    
                    self.logger.info(f"Registered company {record['company_name']} in {record.get('sector', 'unknown')} sector")
                
                except Exception as record_error:
                    self.logger.error(f"Error registering company record: {record_error}")
                    # Continue processing other records
                    continue

        except Exception as e:
            self.logger.error(f"Error in company data registration: {str(e)}")
            raise

def create_company_module(workflow, config=None):
    """
    Factory method to create CompanyModule
    
    :param workflow: BlockchainWorkflow instance
    :return: CompanyModule instance
    """
    return CompanyModule(workflow)