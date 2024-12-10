import pandas as pd
import time
start_time = time.time()
students_df = pd.read_csv('students.csv')  # Assuming 'students.csv' exists
fees_df = pd.read_csv('fees.csv')  # Assuming 'fees.csv' exists
print("Columns in students_df:", students_df.columns)
print("Columns in fees_df:", fees_df.columns)
student_id_index_students = students_df.columns.get_loc('Student ID')  # Get index of 'Student ID' column in students_df
student_id_index_fees = fees_df.columns.get_loc('Student ID')  # Get index of 'Student ID' column in fees_df
print(f"Student ID in students.csv is at index: {student_id_index_students}")
print(f"Student ID in fees.csv is at index: {student_id_index_fees}")
output_data = []  # List to store results
for index, student in students_df.iterrows():  # Iterating over students DataFrame
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
            ' Most Frequent Payment Date': most_frequent_date,
            'Frequency': frequency,
                   })
#  Convert the outputdata list into a DataFrame for display
output_df = pd.DataFrame(output_data)

#  Display the results in the console
print(output_df)
print("Most Recent Date : " ,output_df[' Most Frequent Payment Date'].max())
end_time = time.time()

# Calculate and print execution time
execution_time = end_time - start_time
print(f"Execution Time: {execution_time:.4f} seconds")
