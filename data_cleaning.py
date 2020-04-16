# Import neccessary libraries
from datetime import datetime
import pandas as pd
from metpy.io import parse_metar_to_dataframe
import glob


def parse_metar_file(file, wx_subset=True):
    """
    Parses METAR file from NCDC

    Input:
    --------
    file = Text file downloaded from NCDC

    wx_subset = Flag to determine whether or not to drop non-current weather obs (if True, only returns obs with observed weather)

    Output:
    --------
    df = Pandas dataframe filtered for times where current weather is not 'nan'
    """

    # Read in the file using Pandas
    df = pd.read_csv(file, header=None)

    # Pull the timestamp from the filename
    timestamp = datetime.strptime(file[-10:], '%Y%m.dat')

    # Iterrate over rows to parse METARS
    df_list = []
    for index, row in df.iterrows():
        try:
            df_list.append(parse_metar_to_dataframe(row.values[0][52:], year=timestamp.year, month=timestamp.month))
        except:
            print('Error with METAR: ', row.values[0][52:])
    #
    merged_df = pd.concat(df_list)

    # Drop datasets that do not include current weather
    merged_df = merged_df.dropna(subset=['current_wx1'])

    # Change the index to datetime
    merged_df.index = merged_df.date_time

    # Return the merged dataset sorted by datetime
    return merged_df.sort_index()


# Find all files in the data directory
files = sorted(glob.glob('data/*.dat'))

# Create a list to store the dataframes in
merged_datasets = []

# Loop through and parse the different datasets
for file in files:
    print(file[-10:])
    try:
        merged_datasets.append(parse_metar_file(file))
    except:
        print("Error with :", file)

# Return the cleaned dataset
clean_df = pd.concat(merged_datasets)

# Save the file to memory
clean_df.to_csv('clean_dataset.csv')
# Print out the resulting dataset
print(clean_df)
