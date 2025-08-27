import os
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from fuzzywuzzy import fuzz 
from logging_config import setup_logging
from reqd_vars_dtypes import required_variables, expected_data_types
from clifpy.utils.io import load_data

# Initialize logger
setup_logging()
logger = logging.getLogger(__name__)

# Common Functions

def read_data(file):
    """
    Read data from file using clifpy load_data when possible, fallback to pandas.
    Parameters:
        file: File object with name attribute (like Streamlit UploadedFile).
    Returns:
        DataFrame: DataFrame containing the data.
    """
    logger.info(f"Reading data from file: {file.name}")
    # Determine file type
    if file.name.endswith(".csv"):
        table_format_type = "csv"
    elif file.name.endswith(".parquet"):
        table_format_type = "parquet"
    else:
        logger.error(f"Unsupported file type: {file.name}")
        raise ValueError("Unsupported file type. Please provide either 'csv' or 'parquet'.")
    import tempfile
    
    # Extract table name from filename
    filename = os.path.basename(file.name)
    filename_without_ext = os.path.splitext(filename)[0]
    table_name = filename_without_ext.replace('clif_', '')
    
    logger.debug(f"Table name: {table_name}, Format: {table_format_type}")
    
    # Since Streamlit uploaded files don't have a real path, create a temporary directory
    # and save the file there so clifpy can read it
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create the expected filename that clifpy looks for: clif_[table_name].[extension]
            temp_file_path = os.path.join(temp_dir, f"clif_{table_name}.{table_format_type}")
            
            # Write the uploaded file content to the temporary file
            with open(temp_file_path, 'wb') as temp_file:
                temp_file.write(file.getvalue())
            
            logger.debug(f"Created temporary file: {temp_file_path}")
            
            # Now use clifpy's load_data with the temporary directory
            df = load_data(table_name, temp_dir, table_format_type)
            logger.info(f"Loaded data with shape: {df.shape}")
            return df
            
    except Exception as e:
        logger.warning(f"Failed to load {file.name} using clifpy: {e}. Falling back to pandas.")
        # Fallback to pandas
        if table_format_type == "csv":
            return pd.read_csv(file)
        elif table_format_type == "parquet":
            return pd.read_parquet(file)
    
def check_required_variables(table_name, df): ### Modified from original
    """
    Check if all required variables exist in the DataFrame.
    
    Parameters:
        required_variables (list): List of required variable names.
        df (DataFrame): DataFrame to check.

    Returns:
        list: List of missing variables.
    """
    logger.info(f"Checking required variables for table: {table_name}")
    required_vars = required_variables[table_name]
    missing_variables = [var for var in required_vars if var not in df.columns]
    if missing_variables:
        logger.warning(f"Missing required columns for '{table_name}': {missing_variables}")
        return f"Missing required columns for '{table_name}': {missing_variables}"
    else:
        logger.info(f"All required columns present for '{table_name}'.")
        return f"All required columns present for '{table_name}'."

def generate_summary_stats(data, category_column, value_column):
    """
    Generate summary statistics for a DataFrame based on a specified category column and value column.

    Parameters:
        data (DataFrame): DataFrame containing the data.
        category_column (str): Name of the column containing categories.
        value_column (str): Name of the column containing values.

    Returns:
        DataFrame: DataFrame containing summary statistics.
    """
    logger.info(f"Generating summary stats for {value_column} grouped by {category_column}")
    try:
        summary_stats = data.groupby([category_column]).agg(
            N=(value_column, 'count'),
            Missing=(value_column, lambda x: (x.isnull().sum()/data.shape[0])*100),
            Min=(value_column, 'min'),
            Mean=(value_column, 'mean'),
            Q1=(value_column, lambda x: x.quantile(0.25)),
            Median=(value_column, 'median'),
            Q3=(value_column, lambda x: x.quantile(0.75)),
            Max=(value_column, 'max')
        ).reset_index().sort_values(by=[category_column], ascending=True)

        summary_stats = summary_stats.rename(columns={category_column: 'Category', 'Missing': 'Missing (%)'})
        logger.debug(f"Summary stats generated with shape: {summary_stats.shape}")
        return summary_stats
    except Exception as e:
        logger.error(f"Error generating summary stats: {e}")
        raise

def find_closest_match(label, labels):
    """
    Find the closest match for a label in a list of labels using fuzzy matching.
    """
    logger.debug(f"Finding closest match for label: {label}")
    closest_label = None
    highest_similarity = -1
    for lab_label in labels:
        similarity = fuzz.partial_ratio(label, lab_label)
        logger.debug(f"Comparing '{label}' to '{lab_label}': similarity {similarity}")
        if similarity > highest_similarity:
            closest_label = lab_label
            highest_similarity = similarity
    logger.debug(f"Closest match for '{label}' is '{closest_label}' with similarity {highest_similarity}")
    return closest_label, highest_similarity

def check_categories_exist(data, outlier_thresholds, category_column):
    """
    Check if categories in outlier thresholds match with categories in the data DataFrame.

    Parameters:
        data (DataFrame): DataFrame containing the data.
        outlier_thresholds (DataFrame): DataFrame containing outlier thresholds.
        category_column (str): Name of the column containing categories.

    Returns:
        None
    """
    logger.info(f"Checking if categories in outlier thresholds exist in data for column: {category_column}")
    categories = data[category_column].unique()
    # Lower case categories
    categories = [category.lower() for category in categories]
    logger.debug(f"Categories in data (lowercased): {categories}")
    missing_categories = []
    similar_categories = []

    # Iterate through outlier_thresholds DataFrame
    for _, row in outlier_thresholds.iterrows():
        category = row[category_column]
        logger.debug(f"Checking category '{category}' in outlier thresholds")
        # Check if category exists in data categories
        if category not in categories:
            # If not found, find closest match
            closest_match, similarity = find_closest_match(category, categories)
            if similarity >= 90:  # Set a threshold for similarity score
                logger.info(f"Category '{category}' not found. Closest match: '{closest_match}' (similarity: {similarity})")
                similar_categories.append((category, closest_match))
            else:
                logger.warning(f"Category '{category}' missing in data categories.")
                missing_categories.append(category)  
        else:
            logger.debug(f"Category '{category}' found in data categories.")
    logger.info(f"Similar categories: {similar_categories}, Missing categories: {missing_categories}")
    return similar_categories, missing_categories

def replace_outliers_with_na_long(df, df_outlier_thresholds, category_variable, numeric_variable):
    """
    Replace outliers in the labs DataFrame with NaNs based on outlier thresholds.

    Parameters:
        df (DataFrame): DataFrame containing lab data.
        df_outlier_thresholds (DataFrame): DataFrame containing outlier thresholds.

    Returns:
        DataFrame: Updated DataFrame with outliers replaced with NaNs.
        int: Count of replaced observations.
        float: Proportion of replaced observations.
    """
    logger.info(f"Replacing outliers with NaN for variable '{numeric_variable}' by category '{category_variable}'")
    replaced_count = 0
    outlier_details = []

    for _, row in df_outlier_thresholds.iterrows():
        rclif_category = row[category_variable]
        lower_limit = row['lower_limit']
        upper_limit = row['upper_limit']
        logger.debug(f"Processing category '{rclif_category}' with limits ({lower_limit}, {upper_limit})")
        # Filter DataFrame for the current category
        recorded_values = df.loc[df[category_variable] == rclif_category, numeric_variable]
        # Identify outliers
        outliers = recorded_values[(recorded_values < lower_limit) | (recorded_values > upper_limit)]
        logger.debug(f"Found {len(outliers)} outliers in category '{rclif_category}'")
        # Store outlier details for display
        outlier_details.append((rclif_category, lower_limit, upper_limit, outliers))
        # Replace values outside the specified range with NaNs
        replaced_count += len(outliers)
        df.loc[df[category_variable] == rclif_category, numeric_variable] = np.where((recorded_values < lower_limit) | (recorded_values > upper_limit), np.nan, recorded_values)
        logger.debug(f"Category '{rclif_category}': replaced {len(outliers)} outliers.")

    total_count = len(df)
    proportion_replaced = replaced_count / total_count if total_count > 0 else 0
    logger.info(f"Total outliers replaced: {replaced_count} ({proportion_replaced:.2%})")
    df.reset_index(drop=True, inplace=True)

    logger.debug(f"Outlier details: {outlier_details}")
    return df, replaced_count, proportion_replaced, outlier_details

def replace_outliers_with_na_wide(data, outlier_thresholds):
    """
    Replace outliers with NA values in a DataFrame based on specified lower and upper limits.

    Parameters:
        data (DataFrame): DataFrame containing the data.
        outlier_thresholds (DataFrame): DataFrame containing outlier thresholds.

    Returns:
        DataFrame: DataFrame with outliers replaced by NA values.
        int: Total number of observations replaced with NA.
        float: Proportion of observations replaced with NA.
    """
    logger.info("Replacing outliers with NaN for wide-format data.")
    # Initialize variables to record replaced observations
    total_replaced = 0
    outlier_details = []

    # Iterate over each column in the DataFrame
    for col in outlier_thresholds['variable_name']:
        # Get lower and upper limits for the current column
        lower_limit = outlier_thresholds.loc[outlier_thresholds['variable_name'] == col, 'lower_limit'].values[0]
        upper_limit = outlier_thresholds.loc[outlier_thresholds['variable_name'] == col, 'upper_limit'].values[0]
        logger.debug(f"Processing variable '{col}' with limits ({lower_limit}, {upper_limit})")

        # Replace outliers with NA values
        outliers_mask = (data[col] < lower_limit) | (data[col] > upper_limit)
        outlier_count = outliers_mask.sum()
        logger.debug(f"Found {outlier_count} outliers in variable '{col}'")
        outlier_details.append((col, lower_limit, upper_limit, data.loc[outliers_mask, col]))
        total_replaced += outlier_count
        data.loc[outliers_mask, col] = np.nan
        logger.debug(f"Variable '{col}': replaced {outlier_count} outliers.")

    # Calculate proportion of replaced observations
    total_observations = data.shape[0]
    proportion_replaced = total_replaced / total_observations if total_observations > 0 else 0
    logger.info(f"Total outliers replaced: {total_replaced} ({proportion_replaced:.2%})")
    logger.debug(f"Outlier details: {outlier_details}")

    return data, total_replaced, proportion_replaced, outlier_details

def generate_facetgrid_histograms(data, category_column, value_column):
    """
    Generate histograms using seaborn's FacetGrid.

    Parameters:
        data (DataFrame): DataFrame containing the data.
        category_column (str): Name of the column containing categories.
        value_column (str): Name of the column containing values.

    Returns:
        FacetGrid: Seaborn FacetGrid object containing the generated histograms.
    """
    logger.info(f"Generating FacetGrid histograms for '{value_column}' by '{category_column}'")
    try:
        # Create a FacetGrid
        g = sns.FacetGrid(data, col=category_column, col_wrap=3, sharex=False, sharey=False)
        g.map(sns.histplot, value_column, bins=30, color='dodgerblue', edgecolor='black')

        # Set titles and labels
        g.set_titles(col_template='{col_name}')
        g.set_axis_labels(value_column, 'Frequency')

        # Adjust layout and add a main title
        plt.subplots_adjust(top=0.9, hspace=0.4, wspace=0.4)

        logger.debug("FacetGrid histograms generated.")
        return g
    except Exception as e:
        logger.error(f"Error generating FacetGrid histograms: {e}")
        raise

def non_scientific_format(x):
    """
    Format a number in non-scientific notation with 2 decimal places.
    """
    logger.debug(f"Formatting number {x} in non-scientific notation")
    return '%.2f' % x

def plot_histograms_by_device_category(data, selected_category, selected_mode = None):
    """
    Plot histograms of a variable for a specific device category.

    Parameters:
        data (DataFrame): DataFrame containing the data.
        selected_category (str): Selected device category.
    """
    logger.info(f"Plotting histograms for device category '{selected_category}'" + (f" and mode '{selected_mode}'" if selected_mode else ""))
    variables_to_plot = ["fio2_set", "lpm_set", "tidal_volume_set", "resp_rate_set", 
            "pressure_control_set", "pressure_support_set", "flow_rate_set", 
            "peak_inspiratory_pressure_set", "inspiratory_time_set", "peep_set", 
            "tidal_volume_obs", "resp_rate_obs", "plateau_pressure_obs", 
            "peak_inspiratory_pressure_obs", "peep_obs", "minute_vent_obs"]
    variables_to_plot.sort()
    if selected_mode:
        filtered_df = data[(data['device_category'] == selected_category) & (data['mode_category'] == selected_mode)]
    else:
        filtered_df = data[data['device_category'] == selected_category]
    logger.debug(f"Filtered data shape: {filtered_df.shape}")
    data_melted = filtered_df.melt(value_vars=variables_to_plot, 
                                var_name='Variable', value_name='Value')
    logger.debug(f"Data melted for plotting, shape: {data_melted.shape}")
    try:
        g = sns.FacetGrid(data_melted, col="Variable", col_wrap=4, sharex=False, sharey=False)
        g.map(sns.histplot, "Value", kde=False, bins=20)
        g.set_titles("{col_name}")
        g.set_axis_labels("Value", "Count")
        plt.subplots_adjust(top=0.9, hspace=0.4, wspace=0.4)
        logger.debug("Histograms by device category plotted.")
        return g
    except Exception as e:
        logger.error(f"Error plotting histograms by device category: {e}")
        raise


def validate_and_convert_dtypes(table_name, data):
    """
    Validate and convert data types of columns in the DataFrame 
    based on expected data types.

    Parameters:
        table_name (str): Name of the table.
        data (DataFrame): DataFrame to validate and convert data types.

    Returns:
        DataFrame: The converted DataFrame.
        List: Validation results containing column name, actual dtype, 
              expected dtype, and validation status.
    """
    logger.info(f"Validating and converting dtypes for table: {table_name}")
    expected_dtypes = expected_data_types[table_name]
    validation_results = []

    for column, expected_dtype in expected_dtypes.items():
        if column in data.columns:
            actual_dtype = data[column].dtype

            # Check if the expected type is datetime64
            if expected_dtype == 'datetime64':
                # Ensure the column is in a valid datetime format
                if not pd.api.types.is_datetime64_any_dtype(actual_dtype):
                    validation_results.append((column, actual_dtype, 'datetime64', 'Mismatch'))
                    try:
                        # Attempt to convert to datetime, coerce errors to NaT
                        data[column] = pd.to_datetime(data[column], errors='coerce')
                        logger.info(f"Converted column '{column}' to datetime64.")
                    except Exception as e:
                        logger.error(f"Error converting column {column} to datetime: {e}")
                else:
                    validation_results.append((column, actual_dtype, 'datetime64', 'Match'))

            # Handle non-datetime expected types
            elif str(actual_dtype) != expected_dtype:
                validation_results.append((column, actual_dtype, expected_dtype, 'Mismatch'))
                try:
                    # Convert to the expected dtype
                    if expected_dtype == 'float64':
                        data[column] = pd.to_numeric(data[column], errors='coerce')
                    elif expected_dtype == 'bool':
                        data[column] = data[column].astype('bool')
                    else:
                        data[column] = data[column].astype(expected_dtype)
                    logger.info(f"Converted column '{column}' to {expected_dtype}.")
                except Exception as e:
                    logger.error(f"Error converting column {column} to {expected_dtype}: {e}")
            else:
                validation_results.append((column, actual_dtype, expected_dtype, 'Match'))
        else:
            # Log missing columns
            logger.warning(f"Column '{column}' not found in data.")
            validation_results.append((column, 'Not Found', expected_dtype, 'Missing'))

    logger.info(f"Validation and conversion complete. Results: {validation_results}")
    return data, validation_results         


def name_category_mapping(data):
    """
    Generate mappings between *_name and *_category columns.
    """
    logger.info("Generating name-category mappings.")
    mappings = []
    vars = [col for col in data.columns if col.endswith('_name')]

    for var in vars:
        var_category = var.replace('_name', '_category')
        if var_category in data.columns:
            logger.debug(f"Generating mapping for {var} and {var_category}")
            frequency = data.groupby([var, var_category]).size().reset_index(name='counts')
            frequency = frequency.sort_values(by='counts', ascending=False)
            mappings.append(frequency)
            logger.debug(f"Mapping for {var} and {var_category} generated.")
        else:
            logger.warning(f"Category column '{var_category}' not found for name column '{var}'")
    logger.info(f"Total mappings generated: {len(mappings)}")
    return mappings

def check_time_overlap(data, session):
    try:
        logger.info("Checking for time overlaps in admissions.")
        # Check if 'patient_id' exists in the data
        if 'patient_id' not in data.columns:
            logger.warning("'patient_id' not found in data columns.")
            if "clif_hospitalization" not in session:
                error = "patient_id is missing, and the hospitalization table is not provided."
                logger.error(error)
                return error
            
            hospitalization_table = session["clif_hospitalization"]
            logger.info("Merging with hospitalization table to retrieve patient_id.")
            # Join adt_table with hospitalization_table to get patient_id
            data = data.merge(
                hospitalization_table[['hospitalization_id', 'patient_id']],
                on='hospitalization_id',
                how='left'
            )
            
            # Check if the join was successful
            if 'patient_id' not in data.columns or data['patient_id'].isnull().all():
                error = "Unable to retrieve patient_id after joining with hospitalization_table."
                logger.error(error)
                return error
        
        logger.debug("Sorting data by patient_id and in_dttm.")
        # Sort by patient_id and in_dttm to make comparisons easier
        data = data.sort_values(by=['patient_id', 'in_dttm'])
        
        overlaps = []
        
        # Group by patient_id to compare bookings for each patient
        for patient_id, group in data.groupby('patient_id'):
            logger.debug(f"Checking overlaps for patient_id {patient_id}")
            for i in range(len(group) - 1):
                # Current and next bookings
                current = group.iloc[i]
                next = group.iloc[i + 1]

                # Check if the locations are different and times overlap
                if (
                    current['location_name'] != next['location_name'] and
                    current['out_dttm'] > next['in_dttm']
                ):
                    overlaps.append({
                        'patient_id': patient_id,
                        'Initial Location': (current['location_name'], current['location_category']),
                        'Overlapping Location': (next['location_name'], next['location_name']),
                        'Admission Start': current['in_dttm'],
                        'Admission End': current['out_dttm'],
                        'Next Admission Start': next['in_dttm']
                    })
                    logger.warning(f"Time overlap found for patient_id {patient_id} between {current['location_name']} and {next['location_name']}.")
        
        logger.info(f"Total overlaps found: {len(overlaps)}")
        return overlaps
    
    except Exception as e:
        # Handle errors gracefully
        logger.error(f"Error checking time overlap: {str(e)}")
        raise RuntimeError(f"Error checking time overlap: {str(e)}")
