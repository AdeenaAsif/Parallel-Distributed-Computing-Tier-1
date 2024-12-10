import pandas as pd
import multiprocessing as mp
import numpy as np
import time

# Define a worker function to process a chunk of students
def process_students(chunk, fees_df):
    output_data = []  # List to store results
    for _, student in chunk.iterrows():
        student_id = student['Student ID']  # Get the student ID
        student_name = f"{student['First Name']} {student['Last Name']}"  # Combine first and last name
        student_fees = fees_df[fees_df['Student ID'] == student_id]
        
        if not student_fees.empty:  # If there's a match
            fee_date_counts = student_fees['Date of Fee Paid'].value_counts()
            
            # Find the most frequent fee paid date (the mode)
            most_frequent_date = fee_date_counts.idxmax()  # Get the date with the highest count
            frequency = fee_date_counts.max()  # Get the frequency of the most frequent date
            
            # Append the result to output_data
            output_data.append({
                'Student ID': student_id,
                'Student Name': student_name,
                'Most Frequent Payment Date': most_frequent_date,
                'Frequency': frequency,
            })
    return output_data

if __name__ == "__main__":
    # Start the timer
    start_time = time.time()
    
    # Load the student and fee data
    students_df = pd.read_csv('students.csv')  # Assuming 'students.csv' exists
    fees_df = pd.read_csv('fees.csv')         # Assuming 'fees.csv' exists

    # Convert 'Date of Fee Paid' to datetime
    fees_df['Date of Fee Paid'] = pd.to_datetime(fees_df['Date of Fee Paid'], format='%d/%m/%Y')
    
    # Split the students DataFrame into chunks for multiprocessing
    num_chunks = mp.cpu_count()  # Use the number of available CPU cores
    chunks = np.array_split(students_df, num_chunks)  # Split the students DataFrame
    
    # Create a multiprocessing pool
    with mp.Pool(processes=num_chunks) as pool:
        # Map the chunks to the worker function
        results = pool.starmap(process_students, [(chunk, fees_df) for chunk in chunks])
    
    # Combine the results from all processes
    output_data = [item for sublist in results for item in sublist]
    
    # Convert the output data to a DataFrame
    output_df = pd.DataFrame(output_data)
    
    # Display the results
    print(output_df)
    print("Most Recent Date:", output_df['Most Frequent Payment Date'].max())
    
    # Stop the timer
    end_time = time.time()
    
    # Calculate and print execution time
    execution_time = end_time - start_time
    print(f"Execution Time: {execution_time:.4f} seconds")
