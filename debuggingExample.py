# This is an ETL pipeline that extensively uses logging for efficient debugging
# This is a change in the code.




import logging
import pandas as pd
import matplotlib.pyplot as plt
import dataframe_image as dfi

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def extract_data(file_path):
    print('\n\n')
    logging.info("Data extraction started...")
    data = pd.read_csv(file_path)
    data.set_index('ID', inplace=True)
    logging.info("Data extracted successfully, top 3 rows are:\n %s", data.head(3))
    return data

def transform_data(data):
    logging.info("Data transformation started...")
    data['Age_in_months']=data['Age']*12
    data['Tax'] = data['Salary_GBP']*0.25
    description = data.describe()
    data = data.drop('Name', axis=1)
    logging.info("Generating the plots started...")
    plt.figure()
    plt.scatter(data['Age'], data['Salary_GBP'], marker='o', color='r')
    dfi.export(description,r"C:\Users\WakilSarfaraz\OneDrive - Corndel Ltd\DEContentCreation\DE5-0701\debuggingExample\destination\data_description.png")
    dfi.export(data.corr(),r"C:\Users\WakilSarfaraz\OneDrive - Corndel Ltd\DEContentCreation\DE5-0701\debuggingExample\destination\correlation_matrix.png")
    plt.title(f'Scatter Plot for Age vs Salary', fontsize=15)
    plt.xlabel('Age',fontsize=12)
    plt.ylabel('Annual Salary (GBP)', fontsize=12)
    plt.grid(True)
    plt.savefig(r"C:\Users\WakilSarfaraz\OneDrive - Corndel Ltd\DEContentCreation\DE5-0701\debuggingExample\destination\scatter.png")
    plt.close()
    # Histogram
    plt.figure()
    plt.hist(data['Age'], bins=5, color='g', edgecolor='k', lw=2)
    plt.title(f' Age Histogram', fontsize=15)
    plt.xlabel('Age in Months',fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.grid(True)
    plt.savefig(r"C:\Users\WakilSarfaraz\OneDrive - Corndel Ltd\DEContentCreation\DE5-0701\debuggingExample\destination\Age_histogram.png")
    plt.close()
    transformed_data = data
    logging.info("Data transformed successfully, top 3 rows are:\n %s",transformed_data.head(3))
    return transformed_data


def load_data(df, filename):
    logging.info("Data loading started...")
    df.to_excel(filename, index=False)
    logging.info("Data and graphics loaded and saved successfully in the destination. \n\n")


def run_pipeline():
    data = extract_data(r"C:\Users\WakilSarfaraz\OneDrive - Corndel Ltd\DEContentCreation\DE5-0701\debuggingExample\source\data.csv")
    transformed_data = transform_data(data)
    load_data(transformed_data,filename=r"C:\Users\WakilSarfaraz\OneDrive - Corndel Ltd\DEContentCreation\DE5-0701\debuggingExample\destination\processed_data.xlsx")


if __name__ == "__main__":
    run_pipeline()
