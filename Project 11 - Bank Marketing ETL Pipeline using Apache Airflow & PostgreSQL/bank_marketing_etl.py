import pandas as pd
import numpy as np
from sqlalchemy import create_engine

#extract step
def extract_bank_marketing():
    """
    Extract the bank marketing dataset from postgres database in raw schema

    Returns: DataFrame containing the bank marketing data
    """
    engine = create_engine(
        "##################" # This part is intentionally left incomplete for security reasons
    )
    df = pd.read_sql("SELECT * FROM raw.bank_marketing", engine)

    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
    )

    return df

#transform step
def transform_bank_marketing(df):
    """
    Transform the bank marketing dataset by cleaning and converting data types.
    """
    print("=== RAW DF COLUMNS ===")
    for col in df.columns:
        print(f"[{repr(col)}]")

    # Remove extra spaces in string columns
    # Drop accidental pandas index column if present
    if "index" in df.columns:
        df = df.drop(columns=["index"])
    
    list_columns = list(df.columns)
    for column in list_columns:
        if df[column].dtype == 'object':
            df[column] = df[column].str.strip()
    
    #Job and education column, replace the "." with "_"
    replace_cols = ['job', 'education']
    for col in replace_cols:
        df[col] = df[col].str.replace('.', '_', regex=False)

    #education column, replace the "unknown" with "np.NaN"
    df['education'] = df['education'].replace('unknown', np.NaN)

    #credit_default, mortgage, and campaign_outcome, convert to boolean data type. 1 if "yes", 0 if "no"
    bool_cols = ['credit_default', 'mortgage', 'campaign_outcome']
    for col in bool_cols:
       df[col] = ((df[col] == 'yes').astype(int)).astype('boolean')

    #create last_contact_date column from day, month, year columns
    date_col = ['day', 'month', 'year']
    for col in date_col:
        df[col] = df[col].astype(str)

    df['last_contact_date'] = pd.to_datetime(
        df['year'].astype(str)
        + '-'
        + df['month'].astype(str)
        + '-'
        + df['day'].astype(str)
    )
    
    #save to three dataframes
    client = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']]
    campaign = df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'campaign_outcome', 'last_contact_date']]
    economics = df[['client_id', 'cons_price_idx', 'euribor_three_months']]

    return client, campaign, economics


#validation step
def validate_df(client_df, campaign_df, economics_df):
    """
    Validate the transformed dataframes to ensure data integrity.
    """
    # Check if there's no null values in client_id columns
    assert client_df['client_id'].isnull().sum() == 0, "Error: Null values found in 'client_id' column of client_df."
    assert campaign_df['client_id'].isnull().sum() == 0, "Error: Null values found in 'client_id' column of campaign_df."
    assert economics_df['client_id'].isnull().sum() == 0, "Error: Null values found in 'client_id' column of economics_df."

    # Check for unique client_id in client_df, campaign_df, and economics_df
    assert client_df['client_id'].is_unique, "Error: 'client_id' column in client_df contains duplicate values."
    assert campaign_df['client_id'].is_unique, "Error: 'client_id' column in campaign_df contains duplicate values."
    assert economics_df['client_id'].is_unique, "Error: 'client_id' column in economics_df contains duplicate values."

    # check if the boolean columns contain only True or False values
    bool_columns = ['credit_default', 'mortgage', 'campaign_outcome']
    for col in bool_columns:
        if col in client_df.columns:
            assert client_df[col].dropna().isin([True, False]).all(), f"Error: Column '{col}' in client_df contains values other than True or False."
        if col in campaign_df.columns:
            assert campaign_df[col].dropna().isin([True, False]).all(), f"Error: Column '{col}' in campaign_df contains values other than True or False."
    
    #check if the last_contact_date is in datetime format
    assert campaign_df['last_contact_date'].dtype == 'datetime64[ns]', "Error: 'last_contact_date' column in campaign_df is not in datetime format."

    #check if education column doesn't have 'unknown' values
    assert not client_df['education'].isin(['unknown']).any(), "Error: 'education' column in client_df contains 'unknown' values."

#load step
def load_dataframes_to_postgres(client_df, campaign_df, economics_df):
    """
    Load the transformed dataframes into the postgres database in the cleaned schema.
    """
    engine = create_engine(
        "##################" # This part is intentionally left incomplete for security reasons
    )


    client_df.to_sql('bank_client', engine, schema='cleaned', if_exists='replace', index=False)
    campaign_df.to_sql('bank_campaign', engine, schema='cleaned', if_exists='replace', index=False)
    economics_df.to_sql('bank_economics', engine, schema='cleaned', if_exists='replace', index=False)